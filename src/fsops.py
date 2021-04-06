#!/usr/bin/env python3
import os

import pyfuse3

import errno
import logging
import stat as stat_m
from pyfuse3 import FUSEError
from os import fsencode, fsdecode
from collections import defaultdict
from queue import PriorityQueue
from pathlib import Path
import shutil
import remote

import faulthandler
from stat import filemode
from datetime import datetime
import sys

faulthandler.enable()

log = logging.getLogger(__name__)
from IPython import embed


class col:
	BOLD = '\033[1m'
	B = BOLD

	WHITE = '\033[37m'
	W = WHITE
	BW = BOLD + WHITE

	PURPLE = '\033[95m'
	CYAN = '\033[96m'
	BC = BOLD + CYAN
	DARKCYAN = '\033[36m'

	BLUE = '\033[94m'
	BB = BOLD + BLUE

	GREEN = '\033[92m'
	BG = BOLD + GREEN

	YELLOW = '\033[93m'
	BY = BOLD + YELLOW

	RED = '\033[91m'
	BR = BOLD + RED

	UNDERLINE = '\033[4m'
	END = '\033[0m'


class MaxPrioQueue(PriorityQueue):
	"""
	A Max Heap Queue:
	Shouldnt be used if negative and positive indeces are mixed
	"""

	# As I dont want to fiddle around with inverting items
	# while I have other problems at hand
	def push_nowait(self, item):
		"Same as PriorityQueue.put_nowait()"
		return self.put_nowait((-item[0], item[1]))

	def pop_nowait(self):
		"""Same as PriorityQueue.get_nowait()"""
		index, data = self.get_nowait()
		return (-index, data)


class HSMCacheFS(pyfuse3.Operations):
	__MEGABYTE__ = 1024 * 1024
	enable_writeback_cache = True
	enable_acl = True

	def __init__(self, node: remote.RemoteNode, sourceDir: str, cacheDir: str,
				 metadb=None, log=None, noatime=True, max_cacheSize=4):
		super().__init__()
		# fs related:
		self.sourceDir = sourceDir
		self.cacheDir = cacheDir

		# unused for now:
		# self.metadb = metadb
		# self.log = log
		# self.remote = node

		# inode related:
		self._inode_path_map = {pyfuse3.ROOT_INODE: sourceDir}
		self._inode_path_cache = {pyfuse3.ROOT_INODE: cacheDir}
		self._lookup_cnt = defaultdict(lambda: 0)
		self._fd_inode_map = dict()
		self._inode_fd_map = dict()
		self._fd_open_count = dict()

		# cache related:
		self.current_CacheSize = 0
		self.cacheFill = 0.8
		self.diskUsagePercent = 1
		self.max_cacheSize = max_cacheSize * 512 * self.__MEGABYTE__
		self.time_attr = 'st_mtime_ns' if noatime else 'st_atime_ns'  # remote has mountopt noatime set?

		# initfs
		self.populate_inode_maps()

	def disk_isFull(self):
		return self.disk_isFilledBy(1.0)

	def get_size(self, start_path='.'):
		total_size = 0
		for dirpath, dirnames, filenames in os.walk(start_path):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				# skip if it is symbolic link
				if not os.path.islink(fp):
					total_size += os.path.getsize(fp)
		return total_size

	def disk_isFilledBy(self, percent: float):
		'percent: float needs to be between 0.0 and 1.0'
		assert 0.0 < percent < 1.0, 'disk_isFullBy: needs to be [0-1]'
		diskUsage = self.get_size(self.cacheDir) / self.max_cacheSize
		return True if diskUsage >= percent else False

	def copyIntoCacheDir(self, src: str, dest: str):
		# check if directories leading to dest already exist
		src_p, dest_p = Path(src), Path(dest)
		if src_p.is_dir():
			if not dest_p.exists():
				dest_p.mkdir(parents=True)
		elif src_p.is_file():
			if not dest_p.parent.exists():
				dest_p.mkdir(parents=True)
			shutil.copy2(src_p, dest_p)
		else:
			log.error(f'{col.BY} Unrecognized path: {src_p} -> ignoring')

		# preserve book-keeping
		dest_p.chmod(src_p.stat().st_mode)
		shutil.copystat(src_p, dest_p)
		self.updateCacheSize()

	def disk_canStore(self, path):
		'update Cache Size needs to be called if file is inserted into cacheDir'
		usage = self.get_size
		if os.path.getsize(path) + self.current_CacheSize < self.max_cacheSize:
			return True
		else:
			return False

	def updateCacheSize(self):
		self.current_CacheSize = self.get_size(self.cacheDir)
		self.diskUsagePercent = self.current_CacheSize / self.max_cacheSize

	def print_stat(self, entry):
		'print all common file attributes'
		attr_list = [
			'st_ino', 'st_mode', 'st_nlink', 'st_uid', 'st_gid',
			# 'st_rdev', # root device id is really useless tbh
			'st_size', 'st_blocks',
			self.time_attr, 'st_ctime_ns'
		]
		if sys.platform in ['bsd', 'OS X']:
			attr_list.append('st_birthtime_ns')

		for attr in attr_list:
			attr_val = getattr(entry, attr)
			attr_str = f'{col.BOLD}{attr}: {col.BC}'
			if 'st_mode' == attr:
				attr_str += f'{filemode(attr_val)}'
			elif 'st_size' == attr:
				attr_str += f'{attr_val / 1024:.3} KB'
			elif 'st_blocks' == attr:
				attr_str += f'{attr_val} * 512 B'
			elif 'time_ns' in attr:
				attr_str += f'{datetime.fromtimestamp(attr_val // 1_000_000_000).strftime("%d. %b %Y %H:%M")}'
			else:
				attr_str += f'{attr_val}'
			print(f'{attr_str}{col.END}')

	def populate_inode_maps(self):
		'index the source filesystem tree'
		startpath = self.sourceDir
		transfer_q = MaxPrioQueue()
		for dirpath, dirnames, filenames in os.walk(self.sourceDir):
			dir_attrs = self._getattr(path=dirpath)
			self.print_stat(dir_attrs)

			# atime or mtime
			last_used = getattr(dir_attrs, self.time_attr) // 1_000_000_000
			transfer_q.push_nowait((last_used, (dir_attrs.st_ino, dir_attrs.st_size)))
			self._add_path(dir_attrs.st_ino, dirpath, fromPopulate=True)

			for f in filenames:
				filepath = os.path.join(dirpath, f)
				file_attrs = self._getattr(path=filepath)
				self.print_stat(file_attrs)

				last_used = getattr(file_attrs, self.time_attr) // 1_000_000_000
				transfer_q.push_nowait((last_used, (file_attrs.st_ino, file_attrs.st_size)))
				self._add_path(file_attrs.st_ino, filepath, fromPopulate=True)

		# fetch most recently used until cache is 80% full or no more to fetch necessary
		self.copyRecentFilesIntoCache(transfer_q)

	def copyRecentFilesIntoCache(self, transfer_q: MaxPrioQueue):
		print(f'{col.B}Transfering files...{col.END}')
		while not transfer_q.empty() and not self.disk_isFilledBy(self.cacheFill):
			timestamp, (inode, size) = transfer_q.pop_nowait()
			src = self._inode_path_map[inode]
			dest = src.replace(self.sourceDir, self.cacheDir)
			if self.disk_canStore(src):
				# TODO: might add a progress bar on the bottom side of the terminal
				#		[....C....] currentFileIndex / totalFilesToProcess like pacman
				print(f'{datetime.fromtimestamp(timestamp).strftime("%d. %b %Y %H:%M")},'
					  f'({inode}, {size}) -> {col.BY}{dest}{col.END}')
				self.copyIntoCacheDir(src, dest)
			else:
				print(f'{datetime.fromtimestamp(timestamp).strftime("%d. %b %Y %H:%M")},'
					  f'({inode}, {size}) -> {col.BR}nowhere cache is too FULL{col.END}')
				continue

		percent_full = f'{col.BY}{self.diskUsagePercent:.10f} %{col.END}{col.BW}'
		used_cache_size = f'{col.BY}{(self.current_CacheSize / 1024 / 1024):.2f} MB{col.BW}'
		max_cache_size = f'{col.BY}{(self.max_cacheSize / 1024 / 1024)} MB {col.BW}'
		copy_summary = \
			f'{col.BOLD}Finished transfering. ' \
			f'Cache is now {percent_full} full' \
			f' (used: {used_cache_size} / {max_cache_size}){col.END}'
		print(copy_summary)

	def _inode_to_path(self, inode):
		"""
        simply maps inodes to paths
        raises errno.ENOENT if not in map -> no such file or directory
        """
		# check cache and redirect to cacheDir Path if found
		if val := self.__inode_to_path_from_cache(inode):
			return val
		else:
			# if not self.remote.isMounted():
			#	self.remote.mountRemoteFS()
			try:
				val = self._inode_path_map[inode]
			except KeyError:
				raise FUSEError(errno.ENOENT)  # no such file or directory

		if isinstance(val, set):
			# In case of hardlinks, pick any path
			val = next(iter(val))
		log.debug(col.BG + '_inode_to_path: %d -> %s' + col.END, inode, val)
		return val

	def __inode_to_path_from_cache(self, inode):
		try:
			val = self._inode_path_cache[inode]
		except KeyError:
			# if not in cache the file _is not_ present even in backend
			# as built a file tree in __init__ of the backend
			raise FUSEError(errno.ENOENT)

	def _add_path(self, inode, path, fromPopulate=False):
		if fromPopulate:
			print(f'{col.BC}_add_path: {col.BY} {inode} -> {path}{col.END}')
		# log.debug('_add_path for %d, %s', inode, path)
		self._lookup_cnt[inode] += 1

		# With hardlinks, one inode may map to multiple paths.
		if inode not in self._inode_path_map:
			self._inode_path_cache[inode] = path.replace(self.sourceDir, self.cacheDir)
			self._inode_path_map[inode] = path
			return

		# generate hardlink from path as inode is already in map
		val = self._inode_path_map[inode]
		if isinstance(val, set):
			val.add(path)
		elif val != path:
			self._inode_path_map[inode] = {path, val}

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

	async def __lookup(self, inode_p, name, ctx=None):
		path = os.path.join(self._inode_to_path(inode_p), name)
		attr = self._getattr(path=path)
		if name != '.' and name != '..':
			self._add_path(attr.st_ino, path)
		return attr

	async def lookup(self, inode_p, name, ctx=None):
		name = fsdecode(name)
		# print((col.BOLD + col.RED + 'lookup for %s in %d' + col.END).format(name, inode_p))
		log.debug((col.BOLD + col.RED + 'lookup for %s in %d' + col.END).format(name, inode_p))
		return await self.__lookup(inode_p, name, ctx)

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
		if inode in self._inode_fd_map:  # if isOpened(inode):
			return self._getattr(fd=self._inode_fd_map[inode])
		else:
			return self._getattr(path=self._inode_to_path(inode))

	def _getattr(self, path=None, fd=None):
		assert fd is None or path is None
		assert not (fd is None and path is None)
		try:
			if fd is None:  # get inode attr
				# log.info(col.BY + path + col.END)
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

	# make inode:
	async def mknod(self, inode_p, name, mode, rdev, ctx):
		path = os.path.join(self._inode_to_path(inode_p), fsdecode(name))
		try:
			os.mknod(path, mode=(mode & ~ctx.umask), device=rdev)
			os.chown(path, ctx.uid, ctx.gid)
		except OSError as exc:
			raise FUSEError(exc.errno)
		attr = self._getattr(path=path)
		self._add_path(attr.st_ino, path)
		return attr

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
		stat_.f_namemax = statfs.f_namemax - (len(root) + 1)
		return stat_

	async def opendir(self, inode, ctx):
		return inode

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

	async def readdir(self, inode, off, token):
		# convert inode to path
		path = self._inode_to_path(inode)
		log.debug('reading %s', path)

		# convert to cache_path
		cache_path = path.replace(self.sourceDir, self.cacheDir)
		log.debug(col.BG + 'cache_path: %s, mount_path: %s' + col.END, cache_path, path)

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

	def _forget_path(self, inode, path):
		log.debug('forget %s for %d', path, inode)
		val = self._inode_path_map[inode]
		if isinstance(val, set):
			val.remove(path)
			if len(val) == 1:
				self._inode_path_map[inode] = next(iter(val))
		else:
			del self._inode_path_map[inode]

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
		if inode in self._inode_fd_map:
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
		return (pyfuse3.FileInfo(fh=fd), attr)

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
