#!/usr/bin/python

import os
import pyfuse3
import errno
from IPython import embed
import stat as stat_m
from pyfuse3 import FUSEError
from os import fsencode, fsdecode
from collections import defaultdict
from disk import Disk
from util import Col
import logging
log = logging.getLogger(__name__)


class Dirent:
	# this class should hold any file system information like
	# inode, name, path, attributes
	def __init__(self, path):
		embed()

	def insert(self, item):
		pass

	def stat(self, item):
		pass

	def __contains__(self, item):
		pass

	def search(self, item):
		pass

	def delete(self, item):
		pass



class VFS:
	# I need to save all os operations in this so if a os.lstat is called I can pretend I actually know the stuff
	def __init__(self, sourceDir, cacheDir):
		self._inode_path = {pyfuse3.ROOT_INODE: sourceDir}
		self._inode_path_cache = {pyfuse3.ROOT_INODE: cacheDir}
		self._lookup_cnt = defaultdict(lambda: 0)
		self._fd_inode_map = dict()
		self._inode_fd_map = dict()
		self._fd_open_count = dict()
		self._entry = dict()

	def __contains__(self, item):
		raise ValueError('Noo dont use "in" with this Class. Use inCache or inRemote')

	def inCache(self, item):
		return item in self._inode_path_cache

	def inRemote(self, item):
		return item in self._inode_path



class VFSOps(pyfuse3.Operations):
	_DEFAULT_CACHE_SIZE = 512

	def __init__(self, sourceDir: str, cacheDir: str, maxCacheSizeMB=_DEFAULT_CACHE_SIZE ):
		super().__init__()
		self.disk = Disk(sourceDir, cacheDir, maxCacheSizeMB)
		self.vfs = VFS(sourceDir, cacheDir)
		self.vfs.

		# inode related:
		self._inode_path_map = {pyfuse3.ROOT_INODE: self.disk.sourceDir}
		self._inode_path_cache = {pyfuse3.ROOT_INODE: self.disk.cacheDir}
		self._lookup_cnt = defaultdict(lambda: 0)
		self._fd_inode_map = dict()
		self._inode_fd_map = dict()
		self._fd_open_count = dict()

	# for better readability
	def already_open(self, inode):
		return inode in self._inode_fd_map

	# inode handling
	# inode to path
	def _inode_to_path(self, inode):
		"""
		simply maps inodes to paths
		raises errno.ENOENT if not in map -> no such file or directory
		"""
		# check cache if not in cache raise error as we indexed everything from sourceDir in __init__
		# availability is a different matter we simply check if the file exists at all or not
		try:
			val = self._inode_path_cache[inode]
		except KeyError:
			raise FUSEError(errno.ENOENT)  # no such file or directory

		if isinstance(val, set):
			# In case of hardlinks, pick any path
			val = next(iter(val))
		log.debug(Col.bg(f'_inode_to_path: {inode} -> {val}'))
		return val

	def _add_path(self, inode, path, fromPopulate=False):
		if fromPopulate:
			print(f'{Col.BC}_add_path: {Col.by(f"{inode} -> {path}")}')
		# log.debug('_add_path for %d, %s', inode, path)
		self._lookup_cnt[inode] += 1

		# With hardlinks, one inode may map to multiple paths.
		if inode not in self._inode_path_map:
			self._inode_path_cache[inode] = self.disk.toCachePath(path)
			self._inode_path_map[inode] = self.disk.toSrcPath(path)
			return

		# generate hardlink from path as inode is already in map
		val = self._inode_path_map[inode]
		if isinstance(val, set):
			val.add(path)
		elif val != path:
			self._inode_path_map[inode] = {path, val}

	# special fs methods
	# ==================

	async def statfs(self, ctx):
		root = self._inode_path_map[pyfuse3.ROOT_INODE]
		stat_ = pyfuse3.StatvfsData()
		try:
			statfs = os.statvfs(root)
		except OSError as exc:
			raise FUSEError(exc.errno)
		for attr in ('f_bsize', 'f_frsize', 'f_blocks', 'f_bfree', 'f_bavail',
					 'f_files', 'f_ffree', 'f_favail'):
			setattr(stat_, attr, getattr(statfs, attr))
		stat_.f_namemax = statfs.f_namemax - (len(root.__str__()) + 1)
		return stat_

	async def mknod(self, inode_p, name, mode, rdev, ctx):
		# create special or ordinary file
		# mostly used for fifo / pipes but nowadays mkfifo would be better suited for that
		# mostly rare use cases
		path = os.path.join(self._inode_to_path(inode_p), fsdecode(name))
		try:
			os.mknod(path, mode=(mode & ~ctx.umask), device=rdev)
			os.chown(path, ctx.uid, ctx.gid)
		except OSError as exc:
			raise FUSEError(exc.errno)
		attr = self._getattr(path=path)
		self._add_path(attr.st_ino, path)
		return attr

	# pyfuse3 specific ?
	# ==================

	# inode functions
	async def forget(self, inode_list):
		for (inode, nlookup) in inode_list:
			if self._lookup_cnt[inode] > nlookup:
				self._lookup_cnt[inode] -= nlookup
				continue
			log.debug('forgetting about inode %d', inode)
			assert inode not in self._inode_fd_map
			del self._lookup_cnt[inode]
			try:
				del self._inode_path_map[inode]
			except KeyError:  # may have been deleted
				pass

	def _forget_path(self, inode, path):
		log.debug('forget %s for %d', path, inode)
		val = self._inode_path_map[inode]
		if isinstance(val, set):
			val.remove(path)
			if len(val) == 1:
				self._inode_path_map[inode] = next(iter(val))
		else:
			del self._inode_path_map[inode]

	async def lookup(self, inode_p, name, ctx=None):
		name = fsdecode(name)
		log.debug(Col.br(f'lookup for {name} in {inode_p}'))
		return await self.__lookup(inode_p, name, ctx)

	async def __lookup(self, inode_p, name, ctx=None):
		path = os.path.join(self._inode_to_path(inode_p), name)
		#if not self.disk.isInCache(path):
		#	path = self.disk.toSrcPath(path)
		attr = self._getattr(path=path)
		if name != '.' and name != '..':
			self._add_path(attr.st_ino, path)
		return attr

	# attr methods
	# ============

	async def setattr(self, inode, attr, fields, fh, ctx):
		# We use the f* functions if possible so that we can handle
		# a setattr() call for an inode without associated directory
		# handle.
		if fh is None:
			path_or_fh = self._inode_to_path(inode)
			truncate = os.truncate
			chmod = os.chmod
			chown = os.chown
			stat = os.lstat
		else:
			path_or_fh = fh
			truncate = os.ftruncate
			chmod = os.fchmod
			chown = os.fchown
			stat = os.fstat

		try:
			if fields.update_size:
				truncate(path_or_fh, attr.st_size)

			if fields.update_mode:
				# Under Linux, chmod always resolves symlinks so we should
				# actually never get a setattr() request for a symbolic
				# link.
				assert not stat_m.S_ISLNK(attr.st_mode)
				chmod(path_or_fh, stat_m.S_IMODE(attr.st_mode))

			if fields.update_uid:
				chown(path_or_fh, attr.st_uid, -1, follow_symlinks=False)

			if fields.update_gid:
				chown(path_or_fh, -1, attr.st_gid, follow_symlinks=False)

			if fields.update_atime and fields.update_mtime:
				if fh is None:
					os.utime(path_or_fh, None, follow_symlinks=False,
							 ns=(attr.st_atime_ns, attr.st_mtime_ns))
				else:
					os.utime(path_or_fh, None,
							 ns=(attr.st_atime_ns, attr.st_mtime_ns))
			elif fields.update_atime or fields.update_mtime:
				# We can only set both values, so we first need to retrieve the
				# one that we shouldn't be changing.
				oldstat = stat(path_or_fh)
				if not fields.update_atime:
					attr.st_atime_ns = oldstat.st_atime_ns
				else:
					attr.st_mtime_ns = oldstat.st_mtime_ns
				if fh is None:
					os.utime(path_or_fh, None, follow_symlinks=False,
							 ns=(attr.st_atime_ns, attr.st_mtime_ns))
				else:
					os.utime(path_or_fh, None,
							 ns=(attr.st_atime_ns, attr.st_mtime_ns))

		except OSError as exc:
			raise FUSEError(exc.errno)

		return await self.getattr(inode)

	async def getattr(self, inode, ctx=None):
		if self.already_open(inode):  # if isOpened(inode):
			return self._getattr(fd=self._inode_fd_map[inode])
		else:
			return self._getattr(path=self._inode_to_path(inode))

	def _getattr(self, path=None, fd=None):
		assert fd is None or path is None
		assert not (fd is None and path is None)
		try:
			if fd is None:  # get inode attr
				stat = os.lstat(path)
			else:
				stat = os.fstat(fd)
		except OSError as exc:
			raise FUSEError(exc.errno)

		entry = pyfuse3.EntryAttributes()
		# copy file attributes
		for attr in ('st_ino', 'st_mode', 'st_nlink', 'st_uid', 'st_gid',
					 'st_rdev', 'st_size', 'st_atime_ns', 'st_mtime_ns',
					 'st_ctime_ns'):
			setattr(entry, attr, getattr(stat, attr))  # more general way of entry.'attr' = stat.'attr'
		entry.generation = 0
		entry.entry_timeout = 0
		entry.attr_timeout = 0
		entry.st_blksize = 512
		entry.st_blocks = ((entry.st_size + entry.st_blksize - 1) // entry.st_blksize)

		return entry

	# directory methods
	# =================

	async def mkdir(self, inode_p, name, mode, ctx):
		path = os.path.join(self._inode_to_path(inode_p), fsdecode(name))
		try:
			os.mkdir(path, mode=(mode & ~ctx.umask))
			os.chown(path, ctx.uid, ctx.gid)
		except OSError as exc:
			raise FUSEError(exc.errno)
		attr = self._getattr(path=path)
		self._add_path(attr.st_ino, path)
		return attr

	async def rmdir(self, inode_p, name, ctx):
		name = fsdecode(name)
		parent = self._inode_to_path(inode_p)
		path = os.path.join(parent, name)
		try:
			inode = os.lstat(path).st_ino
			os.rmdir(path)
		except OSError as exc:
			raise FUSEError(exc.errno)
		if inode in self._lookup_cnt:
			self._forget_path(inode, path)

	async def opendir(self, inode, ctx):
		# ctx contains gid, uid, pid and umask
		return inode

	async def readdir(self, inode, off, token):
		# convert inode to path
		path = self._inode_to_path(inode)
		log.debug('reading %s', path)

		# convert to cache_path
		cache_path = self.disk.toCachePath(path)
		log.debug(Col.by(f'cache_path: {cache_path}, mount_path: {path}'))

		# check cache
		if False:
			if os.path.exists(cache_path):
				# in cache
				await self.__readdir(cache_path, off, token)
			else:
				# not in Cache
				if self.remote.isOffline():
					await self.remote.wakeup()
		await self.__readdir(path, off, token)

	async def __readdir(self, path, off, token):
		entries = []
		for name in os.listdir(path):
			if name == '.' or name == '..':
				continue
			attr = self._getattr(path=os.path.join(path, name))
			entries.append((attr.st_ino, name, attr))

		log.debug('read %d entries, starting at %d', len(entries), off)

		# This is not fully posix compatible. If there are hardlinks
		# (two names with the same inode), we don't have a unique
		# offset to start in between them. Note that we cannot simply
		# count entries, because then we would skip over entries
		# (or return them more than once) if the number of directory
		# entries changes between two calls to readdir().
		for (ino, name, attr) in sorted(entries):
			if ino <= off:
				continue
			log.debug(f"{ino} {name}, {attr}")
			if not pyfuse3.readdir_reply(
					token, fsencode(name), attr, ino):
				break
			self._add_path(attr.st_ino, os.path.join(path, name))

	# path methods
	# ============

	async def readlink(self, inode, ctx):
		path = self._inode_to_path(inode)
		try:
			target = os.readlink(path)
		except OSError as exc:
			raise FUSEError(exc.errno)
		return fsencode(target)

	async def link(self, inode, new_inode_p, new_name, ctx):
		new_name = fsdecode(new_name)
		parent = self._inode_to_path(new_inode_p)
		path = os.path.join(parent, new_name)
		try:
			os.link(self._inode_to_path(inode), path, follow_symlinks=False)
		except OSError as exc:
			raise FUSEError(exc.errno)
		self._add_path(inode, path)
		return await self.getattr(inode)

	async def unlink(self, inode_p, name, ctx):
		name = fsdecode(name)
		parent = self._inode_to_path(inode_p)
		path = os.path.join(parent, name)
		try:
			inode = os.lstat(path).st_ino
			os.unlink(path)
		except OSError as exc:
			raise FUSEError(exc.errno)
		if inode in self._lookup_cnt:
			self._forget_path(inode, path)

	async def symlink(self, inode_p, name, target, ctx):
		name = fsdecode(name)
		target = fsdecode(target)
		parent = self._inode_to_path(inode_p)
		path = os.path.join(parent, name)
		try:
			os.symlink(target, path)
			os.chown(path, ctx.uid, ctx.gid, follow_symlinks=False)
		except OSError as exc:
			raise FUSEError(exc.errno)
		stat = os.lstat(path)
		self._add_path(stat.st_ino, path)
		return await self.getattr(stat.st_ino)

	async def rename(self, inode_p_old, name_old, inode_p_new, name_new, flags, ctx):
		if flags != 0:
			raise FUSEError(errno.EINVAL)

		name_old = fsdecode(name_old)
		name_new = fsdecode(name_new)
		parent_old = self._inode_to_path(inode_p_old)
		parent_new = self._inode_to_path(inode_p_new)
		path_old = os.path.join(parent_old, name_old)
		path_new = os.path.join(parent_new, name_new)
		try:
			os.rename(path_old, path_new)
			inode = os.lstat(path_new).st_ino
		except OSError as exc:
			raise FUSEError(exc.errno)
		if inode not in self._lookup_cnt:
			return

		val = self._inode_path_map[inode]
		if isinstance(val, set):
			assert len(val) > 1
			val.add(path_new)
			val.remove(path_old)
		else:
			assert val == path_old
			self._inode_path_map[inode] = path_new

	# utime is not func in pyfuse3

	# File methods
	# ============
	async def open(self, inode, flags, ctx):
		if self.already_open(inode):
			fd = self._inode_fd_map[inode]
			self._fd_open_count[fd] += 1
			return pyfuse3.FileInfo(fh=fd)
		assert flags & os.O_CREAT == 0
		try:
			fd = os.open(self._inode_to_path(inode), flags)
		except OSError as exc:
			raise FUSEError(exc.errno)
		self._inode_fd_map[inode] = fd
		self._fd_inode_map[fd] = inode
		self._fd_open_count[fd] = 1
		# internal_state = \
		#	f"{col.BB}Internal state:{col.END}\n" \
		#	f"_inode_path_map:    {self._inode_path_map}\n" \
		#	f"_inode_path_cache:  {self._inode_path_cache}\n" \
		#	f"_fd_inode_map:      {self._fd_inode_map}\n" \
		#	f"_fd_open_count:     {self._fd_open_count}\n"

		# print(internal_state)
		# print('--------------------')

		# sizeof = os.sys.getsizeof
		# total_memory_usage = sizeof(self._inode_path_map) + sizeof(self._inode_path_cache) + sizeof(self._fd_inode_map) + sizeof(self._fd_open_count)
		# memory_usage = \
		#	f"{col.BB}Internal state:{col.END}\n" \
		#	f"_inode_path_map:    {sizeof(self._inode_path_map)} B\n" \
		#	f"_inode_path_cache:  {sizeof(self._inode_path_cache)} B\n" \
		#	f"_fd_inode_map:      {sizeof(self._fd_inode_map)} B\n" \
		#	f"_fd_open_count:     {sizeof(self._fd_open_count)} B\n" \
		#	f"total_memory_usage: {total_memory_usage/1024:.3f} KB"
		# print(memory_usage)
		return pyfuse3.FileInfo(fh=fd)

	async def create(self, inode_p, name, mode, flags, ctx):
		path = os.path.join(self._inode_to_path(inode_p), fsdecode(name))
		try:
			fd = os.open(path, flags | os.O_CREAT | os.O_TRUNC)
		except OSError as exc:
			raise FUSEError(exc.errno)
		attr = self._getattr(fd=fd)
		self._add_path(attr.st_ino, path)
		self._inode_fd_map[attr.st_ino] = fd
		self._fd_inode_map[fd] = attr.st_ino
		self._fd_open_count[fd] = 1
		return pyfuse3.FileInfo(fh=fd), attr

	async def read(self, fd, offset, length):
		os.lseek(fd, offset, os.SEEK_SET)
		return os.read(fd, length)

	async def write(self, fd, offset, buf):
		os.lseek(fd, offset, os.SEEK_SET)
		return os.write(fd, buf)

	# trunacte is not a function in pyfuse3

	async def release(self, fd):
		if self._fd_open_count[fd] > 1:
			self._fd_open_count[fd] -= 1
			return

		del self._fd_open_count[fd]
		inode = self._fd_inode_map[fd]
		del self._inode_fd_map[inode]
		del self._fd_inode_map[fd]
		try:
			os.close(fd)
		except OSError as exc:
			raise FUSEError(exc.errno)

		# extra methods
		# =============

		#    async def access(self, inode, mode, ctx):
		#        raise FUSEError(errno.ENOSYS)
		#
		#    async def flush(self, fh):
		#        return os.fsync(fh)
		#
		#    async def fsync(self, fh, datasync):
		#        if datasync:
		#            return self.flush(fh)
		#        else: #TODO: read docstring and implement
		#            return self.flush(fh)
		#
		#    async def fsyncdir(self, fh, datasync):
		raise FUSEError(errno.ENOSYS)

	async def releasedir(self, fh):
		# os.sys.stderr.write("\x1b[2J\x1b[H")
		raise FUSEError(errno.ENOSYS)

	# xattr methods
	# =============

	async def setxattr(self, inode, name, value, ctx):
		raise FUSEError(errno.ENOSYS)

	async def getxattr(self, inode, name, ctx):
		raise FUSEError(errno.ENOSYS)

	async def listxattr(self, inode, ctx):
		raise FUSEError(errno.ENOSYS)

	async def removexattr(self, inode, name, ctx):
		raise FUSEError(errno.ENOSYS)
