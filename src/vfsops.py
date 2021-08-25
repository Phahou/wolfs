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
from fileInfo import FileInfo
from pathlib import Path
import re
import remote
#from journal import Journal
from util import __functionName__


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
	def mnt_ino_translation(self, inode):
		return self.disk.mnt_ino_translation(inode)

	def path_to_ino(self, some_path) -> int:
		return self.disk.path_to_ino(some_path)

	async def statfs(self, ctx):
		"""Easisest function to get a entrypoint into the code (not many df calls all around)"""
		root_ino = self.mnt_ino_translation(pyfuse3.ROOT_INODE)
		root = self.vfs.inode_to_cpath(root_ino)
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
		path = os.path.join(self.vfs.inode_to_cpath(inode_p), fsdecode(name))
		log.info(__functionName__(self) + f"{inode_p} {path} mode: {mode}, rdev: {rdev}")
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
		# gets called internally so no translation
		log.debug(f'{__functionName__(self)} {Col.path(path)} for {Col.inode(inode)}')
		val = self.vfs.inode_to_cpath(inode)
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
		def incLookupCount(st_ino):
			if name != '.' and name != '..':
				self.vfs._lookup_cnt[st_ino] += 1

		path: Path = self.vfs.inode_to_cpath(inode_p) / Path(name)

		# check if directory and children are known
		if children := self.vfs.inode_path_map[inode_p].children:
			for child_inode in children:
				if path == self.vfs.inode_to_cpath(child_inode):
					incLookupCount(inode_p)
					assert child_inode == self.vfs.inode_path_map[child_inode].entry.st_ino
					return self.vfs.inode_path_map[child_inode].entry

		# NOENT case: cache negative lookup
		# attr = self.vfs.inode_path_map[inode_p].entry
		attr = pyfuse3.EntryAttributes()
		attr.st_ino = 0
		attr.entry_timeout = VFSOps.__LOOKUP_NOENT_TIMEOUT_IN_SECS
		if not re.findall(r'^.?(folder|cover|convert|tumbler|hidden|jpg|png|jpeg|file|svn)', name,
						  re.IGNORECASE | re.ASCII):
			log.debug(f'Couldnt find {Col.file(name)} in {Col.inode(inode_p)}')
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
		log.info(__functionName__(self) + f" {Col.file(name)} in {Col.inode(inode_p)} with mode {Col.file(mode & ctx.umask)}")
		# works but isnt snappy (folder is only shown after reentering it in thunar)
		path = os.path.join(self.vfs.inode_to_cpath(inode_p), fsdecode(name))
		if inode := self.vfs.getInodeOf(path, inode_p):
			raise FUSEError(errno.EEXIST)
		try:
			# can succeed as dir might not be present in cache
			os.mkdir(path, mode=(mode & ~ctx.umask))
			os.chown(path, ctx.uid, ctx.gid)
		except OSError as exc:
			raise FUSEError(exc.errno)
		attr = FileInfo.getattr(path=path)
		self._add_path(attr.st_ino, path)
		return attr

	async def rmdir(self, inode_p, name, ctx):
		name = fsdecode(name)
		parent = self.vfs.inode_to_cpath(inode_p)
		path = os.path.join(parent, name)
		inode = self.path_to_ino(path)
		log.info(__functionName__(
			self) + f" {Col.inode(inode)}({Col.path(path)}) in {Col.inode(inode_p)}({Col.path(parent)})" + Col.END)
		self.fetchFile(inode)
		try:
			inode = os.lstat(path).st_ino
			os.rmdir(path)
		except OSError as exc:
			raise FUSEError(exc.errno)
		if self.vfs.inLookupCnt(inode):
			self._forget_path(inode, path)

	async def opendir(self, inode, ctx):
		inode = self.mnt_ino_translation(inode)
		log.info(f"{__functionName__(self)} {self.vfs.inode_to_cpath(inode)}")
		log.info(f"{Col.BW}Containing: {Col.file(self.vfs.inode_path_map[inode].children)}")
		# ctx contains gid, uid, pid and umask
		return inode

	async def readdir(self, inode, off, token):
		path = self.vfs.inode_to_cpath(inode)
		log.info(f'{__functionName__(self)} {Col.path(path)}')
		await self.__readdir(inode, off, token)

		await self.__readdir(inode, path, off, token)

	async def __readdir(self, inode: int, path, off, token):
		entries = []
		# copy attributes from src Filesystem
		if childs := self.vfs.inode_path_map[inode].children:
			log.debug(__functionName__(self) + f' searching through {Col.inode(childs)}')
			for child_inode in childs:
				try:
					info: FileInfo = self.vfs.inode_path_map[child_inode]
					# __Very strange__ that `info.entry.st_ino` changes between calls although we dont write to it ?
					# info.entry.st_ino = child_inode

					entries.append((child_inode, info.cache.name, info.entry))
				except KeyError as exc:
					# TODO: ignore missing symlinks for now
					log.error(f'{Col.BR}Ignored FileInfo of {Col.BG}{child_inode}')

		else:
			entries = ()

		s_entries = sorted(entries)

		log.debug('  read %d entries, starting at %d', len(entries), off)
		# This is not fully posix compatible. If there are hardlinks
		# (two names with the same inode), we don't have a unique
		# offset to start in between them. Note that we cannot simply
		# count entries, because then we would skip over entries
		# (or return them more than once) if the number of directory
		# entries changes between two calls to readdir().
		for (ino, name, attr) in s_entries:
			if ino <= off:
				continue
			log.debug(f"    {Col.inode(ino)}, {Col.file(name)}")
			if pyfuse3.readdir_reply(token, fsencode(name), attr, ino):
				self.vfs._lookup_cnt[attr.st_ino] += 1
			else:
				break

	# path methods
	# ============

	def fetchFile(self, inode: int):
		f: Path = self.vfs.inode_to_cpath(inode)
		st_size: int = self.vfs.inode_path_map[inode].entry.st_size
		if not f.exists():
			self.remote.makeAvailable()
			self.__fetchFile(self.disk.toSrcPath(f), st_size)
		return f


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
		Discards one or multiple files to make space for `f`
		Strategry used is to discard the Least recently used files
		:raise pyfuse3.FUSEError  with errno set to according error
		"""
		assert not os.path.islink(f), "open()-syscalls are only bound to files as opendir() exists!"

		# in case the file is bigger than the whole cache size (likely on small cache sizes)
		log.info(f"{__functionName__(self)} {Col.file(f)}")
		if not self.disk.canFit(size):
			log.error('Tried to fetch a file larger than the cache Size Quota')
			raise FUSEError(errno.EDQUOT)

		self.disk.cp2Cache(f, force=True, open_paths=open_paths)

	async def open(self, inode: int, flags: int, ctx):
		inode, inode_old = self.mnt_ino_translation(inode), inode
		log.debug(__functionName__(
			self) + f' {Col.inode(inode)}, flags: {Col.file(flags)}; old_ino: {Col.inode(inode_old)}')

		if self.vfs.already_open(inode):
			fd = self.vfs._inode_fd_map[inode]
			self.vfs._fd_open_count[fd] += 1
			log.info(__functionName__(self), f" (fd, inode): ({fd}, {Col.inode(inode)})")
			return pyfuse3.FileInfo(fh=fd)

		# disable creation handling here
		assert flags & os.O_CREAT == 0

		try:
			info: FileInfo = self.vfs.inode_path_map[inode]
			f = self.fetchFile(inode)

			# File is in Cache now
			fd: int = os.open(f, flags)
			attr = FileInfo.getattr(f)
			attr.st_ino = inode
			info.entry = attr
		except OSError as exc:
			raise FUSEError(exc.errno)

		self.vfs._inode_fd_map[inode] = fd
		self.vfs._fd_inode_map[fd] = inode
		self.vfs._fd_open_count[fd] = 1

		return pyfuse3.FileInfo(fh=fd)

	async def create(self, inode_p, name: str, mode: int, flags: int, ctx):
		inode_p = self.mnt_ino_translation(inode_p)
		path: str = os.path.join(self.vfs.inode_to_cpath(inode_p), fsdecode(name))
		log.debug(__functionName__(self) + ' ' + Col.file(path) + ' in ' + Col.inode(inode_p) + Col.END)
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

	def __unlink_inode(self, inode_p, inode, path):
		info_p: FileInfo = self.vfs.inode_path_map[inode_p]
		assert inode in info_p.children, f"{inode} not in {info_p.children}"
		assert isinstance(info_p.children, list)
		info_p.children.remove(inode)
		self.disk.untrack(path)

	async def unlink(self, inode_p, name, ctx):
		inode_p = self.mnt_ino_translation(inode_p)
		name = fsdecode(name)
		log.debug(__functionName__(self) + f' {Col.file(name)} in  {Col.inode(inode_p)}')
		parent = self.vfs.inode_to_cpath(inode_p)
		path = os.path.join(parent, name)
		try:

			if os.path.exists(path):  # file exists in cache
				os.unlink(path)
			elif 0 == self.vfs.getInodeOf(path, inode_p):
				raise FUSEError(errno.ENOENT)
			inode = self.path_to_ino(path)
			self.__unlink_inode(inode_p, inode, path)

		except OSError as exc:
			raise FUSEError(exc.errno)
		self.journal.unlink(inode, path)
		if self.vfs.inLookupCnt(inode):
			self._forget_path(inode, path)

	async def read(self, fd, offset: int, length: int):
		try:
			os.lseek(fd, offset, os.SEEK_SET)
			return os.read(fd, length)
		except OSError as exc:
			raise FUSEError(exc.errno)

	def __fsync_with_remote(self, cache: Path, flags, write_ops):
		"""Should work with newly created files too as we are re-using the flags"""
		remote = self.disk.toSrcPath(cache)
		fd_cache, fd_remote = os.open(cache, flags), os.open(remote, flags)
		for offset, buflen in write_ops:
			os.lseek(fd_cache, offset, os.SEEK_SET)
			os.lseek(fd_remote, offset, os.SEEK_SET)
			buf = os.read(fd_cache, buflen)
			os.write(fd_remote, buf)
		os.fsync(fd_remote)
		os.close(fd_cache), os.close(fd_remote)

	async def write(self, fd, offset: int, buf: bytes):
		# TODO:
		#   - [x] mark file somewhow as dirty as we have written to it in the cache and they need to be overwritten in the backeend
		#         is probably only a set of paths or more simpler a True / False thing in the inode_path_map
		#         under their entry / attributes
		#   - [ ] maybe cache the write operation(offset, actual_bytes_written) tuple
		#         and sync them later via write ops instead of rewriting the whole file
		#         adv: we dont need a lot of extra space (just 2 ints per dirty file) as we use the file itself but redo everything we did in the cache file
		#         notice: we need to set the attributes to the same values as in the cache then
		os.lseek(fd, offset, os.SEEK_SET)
		# TODO: notice: keep docstring in mind esp. direct_io
		# if errors are encountered exceptions automatically erupt (e.g. MemoryError)
		bytes_written = os.write(fd, buf)

		write_op = (offset, bytes_written)

		# hint for the flush function
		if not self.vfs._fd_dirty_map.get(fd):
			write_history: list = self.vfs._fd_dirty_map[fd]
			write_history.append(write_op)
			self.vfs._fd_dirty_map[fd] = write_history
		else:
			self.vfs._fd_dirty_map[fd] = [write_op]

		return bytes_written

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
		log.warning(f'{self.__class__.__name__}.fsync(): Not implemented')
		raise FUSEError(errno.ENOSYS)

	async def releasedir(self, fh):
		# same as normal release() no more fh are using it
		log.info(f"{__functionName__(self)} {self.vfs.inode_to_cpath(fh)}")
