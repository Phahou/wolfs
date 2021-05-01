#!/usr/bin/python

# suppress 'unused' warnings
from IPython import embed

embed = embed

import os
import pyfuse3
import errno
from pyfuse3 import FUSEError
from os import fsencode, fsdecode
from disk import Disk
from util import Col
import logging
from vfs import VFS
from src.fileInfo import FileInfo
from pathlib import Path
import remote

log = logging.getLogger(__name__)

# ======================================================================================================================
# VFSOps
# ======================================================================================================================

class VFSOps(pyfuse3.Operations):
	_DEFAULT_CACHE_SIZE = 512

	def __init__(self, node: remote.RemoteNode, sourceDir: Path, cacheDir: Path, maxCacheSizeMB=_DEFAULT_CACHE_SIZE,
				 noatime=True):
		super().__init__()
		sourceDir, cacheDir = Path(sourceDir), Path(cacheDir)
		self.disk = Disk(sourceDir, cacheDir, maxCacheSizeMB, noatime)
		self.vfs = VFS(sourceDir, cacheDir)
		self.remote = node

		self.embed_active = False

	# inode handling
	def _inode_to_path(self, inode: int) -> Path:
		return self.vfs.inode_to_path(inode)

	def _add_path(self, inode, path, fromPopulate=False):
		return self.vfs.add_path(inode, path)

	# special fs methods
	# ==================

	async def statfs(self, ctx):
		"""Easisest function to get a entrypoint into the code (not many df calls all around)"""
		self.embed_active = not self.embed_active
		root = self.vfs.inode_to_path(pyfuse3.ROOT_INODE)
		stat_ = pyfuse3.StatvfsData()
		try:
			statfs = os.statvfs(root)
		except OSError as exc:
			raise FUSEError(exc.errno)
		for attr in ('f_bsize', 'f_frsize', 'f_blocks', 'f_bfree', 'f_bavail',
					 'f_files', 'f_ffree', 'f_favail'):
			setattr(stat_, attr, getattr(statfs, attr))
		stat_.f_namemax = statfs.f_namemax - (len(root.__str__()) + 1)
		print(Col.bg(f'RAM-Usage: of _inode_path_map: {self.vfs.getRamUsage()} | ' +
					 f'elements: {str(len(self.vfs._inode_path_map))}'))
		self.disk.printSummary()
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
		attr = FileInfo.getattr(path=path)
		self._add_path(attr.st_ino, path)
		return attr

	# pyfuse3 specific ?
	# ==================

	# inode functions
	async def forget(self, inode_list):
		return await self.vfs.forget(inode_list)

	def _forget_path(self, inode, path):
		log.debug('forget %s for %d', path, inode)
		val = self.vfs.inode_to_path(inode)
		if isinstance(val, set):
			val.remove(path)
			if len(val) == 1:
				self.vfs.set_inode_path(inode, next(iter(val)))
		else:
			self.vfs.del_inode(inode)

	async def lookup(self, inode_p, name, ctx=None):
		name = fsdecode(name)
		log.debug(Col.br(f'lookup for {name} in {inode_p}'))
		return await self.__lookup(inode_p, name, ctx)

	async def __lookup(self, inode_p, name, ctx=None):
		# TODO: look into might cause a bug Im not sure here anymore
		#       errno.NOENT case not implemented
		attr = self.vfs._inode_path_map[inode_p].entry
		return attr

		path = os.path.join(self._inode_to_path(inode_p), name)
		# if not self.disk.isInCache(path):
		#	path = self.disk.toSrcPath(path)
		# attr = FileInfo.getattr(path=path)
		if name != '.' and name != '..':
			# _add_path only as we need to increase lookup count
			self._add_path(attr.st_ino, path)
		return attr

	# attr methods (from vfs)
	# =======================

	async def setattr(self, inode, attr, fields, fh, ctx):
		return await self.vfs.setattr(inode, attr, fields, fh, ctx)

	async def getattr(self, inode, ctx=None):
		return await self.vfs.getattr(inode, ctx)

	# directory methods
	# =================

	async def mkdir(self, inode_p, name, mode, ctx):
		path = os.path.join(self._inode_to_path(inode_p), fsdecode(name))
		try:
			os.mkdir(path, mode=(mode & ~ctx.umask))
			os.chown(path, ctx.uid, ctx.gid)
		except OSError as exc:
			raise FUSEError(exc.errno)
		attr = FileInfo.getattr(path=path)
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
		if self.vfs.inLookupCnt(inode):
			self._forget_path(inode, path)

	async def opendir(self, inode, ctx):
		# ctx contains gid, uid, pid and umask
		return inode

	async def readdir(self, inode, off, token):
		path = self._inode_to_path(inode)
		log.debug('reading %s', path)

		await self.__readdir(inode, path, off, token)

	async def __readdir(self, inode: int, path, off, token):
		entries = []
		# copy attributes from src Filesystem
		if childs := self.vfs._inode_path_map[inode]._childs:
			for child_inode in childs:
				try:
					info = self.vfs._inode_path_map[child_inode]
				except KeyError as exc:
					# TODO: ignore missing symlinks for now
					pass
				entries.append((child_inode, info.cache.name, info.entry))
		else:
			entries = ()

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
		if self.vfs.inLookupCnt(inode):
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
		if not self.vfs.inLookupCnt(inode):
			return

		val = self.vfs._inode_path_map[inode].src
		if isinstance(val, set):
			assert len(val) > 1
			val.add(path_new)
			val.remove(path_old)
		else:
			assert val == path_old
			self.vfs.set_inode_path(inode, path_new)

	# utime is not func in pyfuse3

	# File methods (functions with file descriptors)
	# ==============================================

	def __fetchFile(self, f: Path, size: int):
		"""
		Discards one or multiple files to make space for :f:
		Strategry used is to discard the Least recently used files
		:return list of discarded Paths
		:raise pyfuse3.FUSEError
		"""
		print(f'__fetchFile: {f}')
		# in case the file is bigger than the whole cache size (unlikely but possible on small sizes)
		if not self.disk.canFit(size):
			log.error('Tried to fetch a file larger than the cache Size Quota')
			raise FUSEError(errno.EDQUOT)
		elif os.path.islink(f):
			# open syscalls are only bound to files as opendir() exists
			raise FUSEError(errno.EISDIR)

		self.disk.cp2Cache(f, force=True)

	async def open(self, inode: int, flags: int, ctx):
		# return early for already opened fd
		print(f'open({inode}, {flags}, {ctx})')

		if self.vfs.already_open(inode):
			fd = self.vfs._inode_fd_map[inode]
			self.vfs._fd_open_count[fd] += 1
			print(Col.by(f'open: (fd, inode): ({fd}, {inode})'))
			return pyfuse3.FileInfo(fh=fd)

		# disable creation handling here
		assert flags & os.O_CREAT == 0

		try:
			f: Path = self._inode_to_path(inode)
			info: FileInfo = self.vfs._inode_path_map[inode]

			# fetch file from remote if not in cache already
			if not f.exists():
				f_src = self.disk.toSrcPath(f)
				# self.remote.makeAvailable()
				try:
					self.__fetchFile(f_src, info.entry.st_size)
				except FUSEError as e:
					print('Something really bad happened')

			# File is in Cache now
			fd: int = os.open(f, flags)
			info.updateEntry(fd=fd)
		except OSError as exc:
			raise FUSEError(exc.errno)

		self.vfs._inode_fd_map[inode] = fd
		self.vfs._fd_inode_map[fd] = inode
		self.vfs._fd_open_count[fd] = 1

		return pyfuse3.FileInfo(fh=fd)

	async def create(self, inode_p, name: str, mode: int, flags: int, ctx):
		path: str = os.path.join(self._inode_to_path(inode_p), fsdecode(name))
		try:
			fd: int = os.open(path, flags | os.O_CREAT | os.O_TRUNC)
		except OSError as exc:
			raise FUSEError(exc.errno)
		attr = FileInfo.getattr(fd=fd)
		self._add_path(attr.st_ino, path)
		self.disk.addFile(path, attr)
		self.vfs._inode_fd_map[attr.st_ino] = fd
		self.vfs._fd_inode_map[fd] = attr.st_ino
		self.vfs._fd_open_count[fd] = 1
		return pyfuse3.FileInfo(fh=fd), attr

	async def read(self, fd, offset: int, length: int):
		os.lseek(fd, offset, os.SEEK_SET)
		return os.read(fd, length)

	async def write(self, fd, offset, buf):
		os.lseek(fd, offset, os.SEEK_SET)
		return os.write(fd, buf)

	# trunacte is not a function in pyfuse3

	async def release(self, fd):
		if self.vfs._fd_open_count[fd] > 1:
			self.vfs._fd_open_count[fd] -= 1
			return

		del self.vfs._fd_open_count[fd]
		inode = self.vfs._fd_inode_map[fd]
		del self.vfs._inode_fd_map[inode]
		del self.vfs._fd_inode_map[fd]
		try:
			os.close(fd)
		except OSError as exc:
			raise FUSEError(exc.errno)

	# extra methods
	# =============

	async def access(self, inode, mode, ctx):
		# for permissions but eh
		raise FUSEError(errno.ENOSYS)

	#
	async def flush(self, fh):
		# 'the close syscall'
		# might be interesting to look into as if we might run out of space we need to
		# wakeup the backend for sync
		if write_ops := self.vfs._fd_dirty_map[fh]:
			inode: int = self.vfs._fd_inode_map[fh]
			info: FileInfo = self.vfs._inode_path_map[inode]
			info.write_ops = write_ops
			self.vfs._fd_inode_map[fh] = None
		# TODO: sync up later (timer would probably be the best choice or
		#  		some kind of check if there is almost no space available on underlying cache disk)

		# TODO: :notice: difference between flush and fsync:
		#	    flush: data _to be written_ to disk
		#       fsync: data _is written_ to disk
		#	need to think about if fsync shall be used to write to backeend directly
		#	mhm if I call fsync here it is already commited to disk
		#   the programs above dont need to know if something is on a not accessible drive or not tbh
		#   -> fsync it is
		return os.fsync(fh)

	async def fsync(self, fh, datasync):

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
