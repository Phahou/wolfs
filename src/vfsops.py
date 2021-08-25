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
	__LOOKUP_NOENT_TIMEOUT_IN_SECS = 5

	def __init__(self, node: remote.RemoteNode, sourceDir: Path, cacheDir: Path,
				 logFile: Path, maxCacheSizeMB=_DEFAULT_CACHE_SIZE, noatime=True):
		super().__init__()
		sourceDir, cacheDir = Path(sourceDir), Path(cacheDir)
		self.disk = Disk(sourceDir, cacheDir, maxCacheSizeMB, noatime)
		self.vfs = VFS(sourceDir, cacheDir)
		self.journal = Journal(self.disk, self.vfs, logFile)
		self.remote = node

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
		log.info( f"{__functionName__(self)}: elements in RAM: {Col.path(len(self.vfs.inode_path_map))}" )
		if not self.journal.isCompletelyClean():
			log.info(self.disk.getSummary())
		self.journal.flushCompleteJournal()
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
		attr.st_ino = self.path_to_ino(path)
		self.vfs.add_path(attr.st_ino, path)
		return attr

	# pyfuse3 specific ?
	# ==================

	# inode functions
	async def forget(self, inode_list):
		translated_ino_list = []
		inode_list = sorted(inode_list)
		for (ino, nlookup) in inode_list:
			translated_ino_list.append((self.mnt_ino_translation(ino), nlookup))
		log.debug(__functionName__(
			self) + f' for untranslated: {Col.inode(inode_list)} -> {Col.inode(translated_ino_list)}(translated)' + Col.END)
		return await self.vfs.forget(translated_ino_list)

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

	# done?
	async def lookup(self, inode_p, name, ctx=None):
		name = fsdecode(name)
		inode_p = self.mnt_ino_translation(inode_p)
		# why does it use the inode numbers of the cache directory ?
		# ignore some lookups when debugging
		if not re.findall(r'^.?(folder|cover|convert|tumbler|hidden|jpg|png|jpeg|file|svn)', name,
						  re.IGNORECASE | re.ASCII):
			log.debug(f'{__functionName__(self)} for {Col.file(name)} in {Col.inode(inode_p)}')
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
		inode_untranslated = inode
		inode = self.mnt_ino_translation(inode_untranslated)
		log.info(f'{__functionName__(self)} for {Col.inode(inode)}(translated from: {Col.inode(inode_untranslated)})')
		if self.disk.in_cache.get(inode)
		entry = await self.vfs.getattr(inode, ctx)
		path = self.vfs.inode_to_cpath(inode)
		entry.st_ino = self.path_to_ino(path)
		return entry

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
		attr.st_ino = self.path_to_ino(path)
		self.disk.track(path)
		self.vfs.addFilePath(inode_p, attr.st_ino, path, attr)
		self.journal.mkdir(attr.st_ino, path, mode)
		return attr

	async def rmdir(self, inode_p, name, ctx):
		# TODO: log on success
		#       die.net: Upon successful completion, the rmdir() function shall mark for update the st_ctime and st_mtime fields of the parent directory.
		#       -> update entries
		inode_p = self.mnt_ino_translation(inode_p)

		name = fsdecode(name)
		parent = self.vfs.inode_to_cpath(inode_p)
		path = os.path.join(parent, name)
		inode = self.path_to_ino(path)
		log.info(__functionName__(
			self) + f" {Col.inode(inode)}({Col.path(path)}) in {Col.inode(inode_p)}({Col.path(parent)})" + Col.END)
		self.fetchFile(inode)
		try:
			os.rmdir(path)
		except OSError as exc:
			raise FUSEError(exc.errno)

		self.journal.rmdir(inode, path)
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

	async def __readdir(self, inode: int, off: int, token):
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

		def rename_childs(childs, parent_new: str):
			for child in childs:
				cache: Path = self.vfs.inode_path_map[child].cache
				self.vfs.inode_path_map[child].cache = Path(cache.__str__().replace(cache.parent.__str__(), parent_new))
				rename_childs(self.vfs.inode_path_map[child].children, parent_new)

		def logMsg():
			log.debug(__functionName__(self, 2) + f'Trying to delete {Col.inode(ino_old)}({Col.path(path_old)}) ' +
					  f'from {Col.file(info_old_p.children)} ({Col.path(info_old_p.cache)})')

		# TODO: https://linux.die.net/man/3/rename
		#       could be made completly in memory as nothing is opened written
		#       to except the parent dir (which is already in memory)
		#       ----
		#       flags could cause an issue later on though so they are disabled for now
		# get everything we need
		join, ino2Path = os.path.join, self.vfs.inode_to_cpath
		inoPathMap: dict[int, FileInfo] = self.vfs.inode_path_map

		inode_p_old, inode_p_new = self.mnt_ino_translation(inode_p_old), self.mnt_ino_translation(inode_p_new)
		path_old, path_new = join(ino2Path(inode_p_old), fsdecode(name_old)), join(ino2Path(inode_p_new),
																				   fsdecode(name_new))
		ino_old = self.vfs.getInodeOf(path_old, inode_p_old)
		# ino_old = self.path_to_ino(path_old)

		if os.path.exists(path_new):  # calls lookup and fails if path_new will be overwritten
			raise FUSEError(errno.EINVAL)

		log.info(__functionName__(self) + f" {Col.file(path_old)} -> {Col.file(path_new)}")
		self.fetchFile(ino_old)

		try:
			os.rename(path_old, path_new)  # fails if file not in cachedir!
		except OSError as exc:
			raise FUSEError(exc.errno)

		# file is renamed now we need to update our internal entries
		info_old_p: FileInfo = inoPathMap[inode_p_old]
		info_new_p: FileInfo = inoPathMap[inode_p_new]
		logMsg()

		# remove from old parent
		info_old_p.children.remove(ino_old)
		self.disk.untrack(path_old)

		# add to new parent
		info_new_p.children.append(ino_old)
		self.disk.track(path_new)

		info_ino_old: FileInfo = inoPathMap[ino_old]
		# move FileInfo to new inode. Journal changes src if synced
		info_ino_old.cache = Path(path_new)  # no hardlinks atm this should be fine
		self.journal.rename(ino_old, path_old, path_new)
		if os.path.isdir(path_new):
			rename_childs(info_ino_old.children, ino2Path(inode_p_new).__str__())

		if self.vfs.inLookupCnt(ino_old):
			self.vfs._lookup_cnt[ino_old] += 1

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

		open_paths, write_ops_reserved_size = self.journal.getDirtyPaths()
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
		attr.st_ino = self.path_to_ino(path)
		self.vfs.addFilePath(inode_p, attr.st_ino, path, attr)
		self.disk.track(path, force=True)

		self.vfs._inode_fd_map[attr.st_ino] = fd
		self.vfs._fd_inode_map[fd] = attr.st_ino
		self.vfs._fd_open_count[fd] = 1
		self.journal.create(attr.st_ino, path, flags | os.O_CREAT | os.O_TRUNC)
		# TODO: check if the same
		f: pyfuse3.FileInfo = pyfuse3.FileInfo(fh=fd)
		return f, attr

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
		try:
			os.lseek(fd, offset, os.SEEK_SET)
			# TODO: notice: keep docstring in mind esp. direct_io
			# if errors are encountered exceptions automatically erupt (e.g. MemoryError)
			bytes_written = os.write(fd, buf)

			# as we might crash without notice it is paramount to be able to
			# replay the write_ops without knowning fd<->inode relation
			# so we use inodes instead of fds ...
			inode: int = self.vfs._fd_inode_map.get(fd)
			self.journal.write(inode, offset, bytes_written)
			return bytes_written
		except OSError as exc:
			raise FUSEError(exc.errno)

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
		inode: int = self.vfs._fd_inode_map.get(fh)
		self.journal.flush(inode, fh)  # store write history for later sync
		return os.fsync(fh)  # data is only written to cache_dir

	async def fsync(self, fh, datasync):
		log.warning(f'{self.__class__.__name__}.fsync(): Not implemented')
		raise FUSEError(errno.ENOSYS)

	async def releasedir(self, fh):
		# same as normal release() no more fh are using it
		log.info(f"{__functionName__(self)} {self.vfs.inode_to_cpath(fh)}")
