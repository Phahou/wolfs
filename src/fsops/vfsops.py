#!/usr/bin/python
# job of this module:
# - interface to add/delete paths to internal data structures
# - get a file from the backend
# - basic operations for the filesystem

from src.libwolfs.errors import SOFTLINK_DISABLED_ERROR
import os
import pyfuse3
from pyfuse3 import ROOT_INODE as FUSE_ROOT_INODE

import errno
from pyfuse3 import FUSEError
from os import fsdecode
from src.libwolfs.disk import Disk
from src.libwolfs.translator import DiskBase, MountFSDirectoryInfo
from src.libwolfs.util import Col
from src.libwolfs.vfs import VFS
from src.libwolfs.fileInfo import FileInfo, DirInfo
from pathlib import Path
from typing import Final, cast, Optional
import re
from src.remote import RemoteNode  # type: ignore
from src.libwolfs.journal import Journal
from src.libwolfs.util import CallStackAware

import logging

log = logging.getLogger(__name__)


# ======================================================================================================================
# VFSOps
# ======================================================================================================================

class VFSOps(pyfuse3.Operations, CallStackAware):
	_DEFAULT_CACHE_SIZE: Final[int] = 512
	_STDOUT: Final[str] = "/dev/stdout"

	def __getitem__(self, inode: int) -> int:
		"""translate pyfuse3.ROOT_INODE into our ROOT_INODE"""
		return inode if inode != FUSE_ROOT_INODE else DiskBase.ROOT_INODE

	def __init__(self, node: RemoteNode, mount_info: MountFSDirectoryInfo,
				 logFile: Path = "", maxCacheSizeMB: int = _DEFAULT_CACHE_SIZE, noatime: bool = True):
		super().__init__()
		self.disk = Disk(mount_info, maxCacheSizeMB, noatime)
		self.vfs = VFS(mount_info)
		self.journal = Journal(self.disk, self.vfs, logFile)
		self.remote = node

	# path methods
	# ============
	def add_subDirectories(self, child_inodes: list[int],
		inode_p: int = 0, wolfs_inode_path: str = "") -> DirInfo:
		trans = self.disk.trans
		assert inode_p >= 0\
			or (wolfs_inode_path != "" and trans.toRoot(wolfs_inode_path) in trans._InodeTranslator__path_ino_map)

		if inode_p > 0:
			# TODO: path_to_ino -> inode_to_cpath should be in the same class as they are interchangeable
			assert_path: str = self.disk.trans.toRoot(self.vfs.cpath(inode_p))
			assert inode_p == trans.path_to_ino(assert_path)
		else:
			inode_p = trans.path_to_ino(wolfs_inode_path)
			assert inode_p == self.vfs.cpath(inode_p)

		assert inode_p not in child_inodes
		assert inode_p in self.vfs.inode_path_map

		# both should be consitent from here on
		directory: DirInfo = self.vfs.inode_path_map[inode_p]
		assert child_inodes not in directory.children
		directory.children += child_inodes
		return directory

	def add_Directory(self, path: str) -> DirInfo:
		wolfs_inode: int = self.disk.trans.path_to_ino(path)

		# get inode info about parent

		inode_p_path = self.disk.trans.getParent(path)
		inode_p = self.disk.trans.path_to_ino(inode_p_path)

		entry = FileInfo.getattr(path=path)
		entry.st_ino = wolfs_inode

		# consitency checks
		assert wolfs_inode != inode_p \
			or wolfs_inode == DiskBase.ROOT_INODE  # exception so that '/' redirects to itself
		assert entry.st_ino == wolfs_inode
		assert Path(path).is_dir()
		assert self.disk.trans.path_to_ino(inode_p_path) == inode_p

		return self.vfs._add_Directory(inode_p, wolfs_inode, path, entry)

	def __fetchFile(self, f: Path, size: int) -> None:
		"""
		Discards one or multiple files to make space for `f`
		Strategry used is to discard the Least recently used files
		:raise pyfuse3.FUSEError  with errno set to according error
		"""
		assert not os.path.islink(f), SOFTLINK_DISABLED_ERROR

		# in case the file is bigger than the whole cache size (likely on small cache sizes)
		log.info(f"{Col(f)}")
		if size > self.disk:
			log.error('Tried to fetch a file larger than the cache Size Quota')
			raise FUSEError(errno.EDQUOT)

		open_paths, write_ops_reserved_size = self.journal.getDirtyPaths()
		self.disk.cp2Cache(f, force=True, open_paths=open_paths)

	def fetchFile(self, inode: int) -> Path:
		f: Path = self.vfs.cpath(inode)
		st_size: int = self.vfs.inode_path_map[inode].entry.st_size
		if not f.exists():
			self.remote.makeAvailable()
			self.__fetchFile(self.disk.trans.toSrc(f), st_size)
		return f

	async def rename(self,
			inode_p_old: int,
			name_old: str,
			inode_p_new: int,
			name_new: str,
			flags: int,
			ctx: pyfuse3.RequestContext) -> None:
		# if inode_p_old == inode_p_new:
		# 	# just rename the paths
		# 	path: str = self.disk.trans.ino_to_path(inode_p_old)

		if flags != 0:
			raise FUSEError(errno.EINVAL)

		def rename_childs(childs: list[int], parent_new: str) -> None:
			for child in childs:
				child_info = self.vfs.inode_path_map[child]
				cache_disk_path: Path = self.disk.toTmp(self.disk.ino_to_rpath(child_info.entry.st_ino))
				cache: Path = child_info.cache
				assert cache == cache_disk_path, "Consistency Error"
				assert isinstance(cache, Path), f"{self} {SOFTLINK_DISABLED_ERROR}"
				self.vfs.inode_path_map[child].cache = Path(cache.__str__().replace(cache.parent.__str__(), parent_new))
				if isinstance(child_info, DirInfo):
					rename_childs(child_info.children, parent_new)

		# TODO: https://linux.die.net/man/3/rename
		#       could be made completly in memory as nothing is opened written
		#       to except the parent dir (which is already in memory)
		#       ----
		#       flags could cause an issue later on though so they are disabled for now
		# get everything we need
		join, ino2Path = os.path.join, self.vfs.cpath
		inoPathMap: dict[int, FileInfo] = self.vfs.inode_path_map

		inode_p_old, inode_p_new = self[inode_p_old], self[inode_p_new]
		path_old, path_new = join(ino2Path(inode_p_old), fsdecode(name_old)), join(ino2Path(inode_p_new),
																				   fsdecode(name_new))
		ino_old = self.disk.path_to_ino(path_old, reuse_ino=inode_p_old)

		if os.path.exists(path_new):  # calls lookup and fails if path_new will be overwritten
			raise FUSEError(errno.EINVAL)

		log.info(f"{Col(path_old)} -> {Col(path_new)}")
		self.fetchFile(ino_old)

		try:
			os.rename(path_old, path_new)  # fails if file not in cachedir!
		except OSError as exc:
			raise FUSEError(exc.errno)

		# file is renamed now we need to update our internal entries
		info_old_p: DirInfo = cast(DirInfo, inoPathMap[inode_p_old])

		old_children: list[int] = info_old_p.children
		new_children: list[int] = cast(DirInfo, inoPathMap[inode_p_new]).children

		def logMsg(ino_old: int, path_old: str, children: list[int], cache_path: str) -> None:
			ino_old, path_old = Col(ino_old), Col(path_old)
			children, cache_path = Col(children), Col(cache_path)
			log.debug(f'Trying to delete {ino_old}({path_old}) from {children} ({cache_path})')
		info_old_p_cache_path: str = self.disk.toTmp(self.disk.ino_to_rpath(info_old_p.entry.st_ino)).__str__()
		logMsg(inode_p_old, path_old, old_children, info_old_p.cache.__str__())
		logMsg(inode_p_old, path_old, old_children, info_old_p_cache_path)

		# remove from old parent
		old_children.remove(ino_old)
		self.disk.untrack(path_old)

		# add to new parent
		new_children.append(ino_old)
		self.disk.track(path_new, reuse_ino=ino_old)

		info_ino_old: DirInfo = cast(DirInfo, inoPathMap[ino_old])
		# move FileInfo to new inode. Journal changes src if synced
		info_ino_old.cache = Path(path_new)  # no hardlinks atm this should be fine
		self.journal.log_rename(ino_old, path_old, path_new)
		if os.path.isdir(path_new):
			rename_childs(info_ino_old.children, ino2Path(inode_p_new).__str__())

		if self.vfs.inLookupCnt(ino_old):
			self.vfs._lookup_cnt[ino_old] += 1


class BasicOps(VFSOps):
	__LOOKUP_NOENT_TIMEOUT_IN_SECS: Final[int] = 5

	def update_refs(self, fd: int, inode: int):
		"""in open & create functions"""
		self.vfs._inode_fd_map[inode] = fd
		self.vfs._fd_inode_map[fd] = inode
		self.vfs._fd_open_count[fd] = 1

	# pyfuse3 specific ?
	# ==================

	# inode functions
	async def forget(self, inode_list: list[tuple[int, int]]) -> None:
		translated_ino_list: list[tuple[int, int]] = []
		sorted_inodes: list[tuple[int, int]] = sorted(inode_list)
		for (ino, nlookup) in sorted_inodes:
			translated_ino_list.append((self[ino], nlookup))
		log.debug(f' for untranslated: {Col(sorted_inodes)} -> {Col(translated_ino_list)} (translated)')
		await self.vfs.forget(translated_ino_list)

	def _forget_path(self, inode: int, path: str) -> None:
		# gets called internally so no translation
		log.debug(f'{Col(path)} as ino {Col(inode)}')
		val = self.vfs.cpath(inode)
		if isinstance(val, set):
			val.remove(path)
			if len(val) == 1:
				self.vfs.set_inode_path(inode, next(iter(val)))
		else:
			self.vfs.del_inode(inode)
			del self.disk.trans[(inode, path)]

	# done?
	async def lookup(self, inode_p: int, name: str, ctx: pyfuse3.RequestContext = None) -> pyfuse3.EntryAttributes:
		name = fsdecode(name)
		inode_p = self[inode_p]

		# ignore some lookups when debugging
		if not re.findall(r'^.?(folder|cover|convert|tumbler|hidden|jpg|png|jpeg|file|svn)', name,
						  re.IGNORECASE | re.ASCII):
			log.debug(f'for {Col(name)} in {Col(inode_p)}')
		return await self.__lookup(inode_p, name, ctx)

	async def __lookup(self, inode_p: int, name: str, ctx: pyfuse3.RequestContext = None) -> pyfuse3.EntryAttributes:

		def incLookupCount(st_ino: int) -> None:
			if name != '.' and name != '..':
				self.vfs._lookup_cnt[st_ino] += 1

		path: Path = self.vfs.cpath(inode_p) / Path(name)

		# check if directory and children are known
		info = self.vfs.inode_path_map[inode_p]
		if isinstance(info, DirInfo):
			for child_inode in info.children:
				if path == self.vfs.cpath(child_inode):
					incLookupCount(inode_p)
					assert child_inode == self.vfs.inode_path_map[child_inode].entry.st_ino
					return self.vfs.inode_path_map[child_inode].entry

		# NOENT case: cache negative lookup
		# attr = self.vfs.inode_path_map[inode_p].entry
		attr = pyfuse3.EntryAttributes()
		attr.st_ino = 0
		attr.entry_timeout = self.__LOOKUP_NOENT_TIMEOUT_IN_SECS
		if not re.findall(r'^.?(folder|cover|convert|tumbler|hidden|jpg|png|jpeg|file|svn)', name,
						  re.IGNORECASE | re.ASCII):
			log.debug(f'Couldnt find {Col(name)} in {Col(inode_p)}')
		return attr

	# attr methods (from vfs)
	# =======================

	async def setattr(self, inode: int, attr: pyfuse3.EntryAttributes, fields: pyfuse3.SetattrFields, fh: int,
					  ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
		return await self.vfs.setattr(inode, attr, fields, fh, ctx)

	async def getattr(self, inode: int, ctx: pyfuse3.RequestContext = None) -> pyfuse3.EntryAttributes:
		inode = self[inode]
		entry = await self.vfs.getattr(inode, ctx)
		path = self.vfs.cpath(inode)
		entry.st_ino = self.disk.trans.path_to_ino(path)
		return entry

	# File methods (functions with file descriptors)
	# ==============================================

	async def open(self, inode: int, flags: int, ctx: pyfuse3.RequestContext) -> pyfuse3.FileInfo:
		inode, inode_old = self[inode], inode
		log.debug(f'{Col(inode)}, flags: {Col(flags)}; old_ino: {Col(inode_old)}')

		if inode in self.vfs._inode_fd_map:
			fd: int = self.vfs._inode_fd_map[inode]
			self.vfs._fd_open_count[fd] += 1
			# log.info(self + f" (fd, inode): ({fd}, {Col.inode(inode)})")
			return pyfuse3.FileInfo(fh=fd)

		# disable creation handling here
		assert flags & os.O_CREAT == 0

		try:
			info: FileInfo = self.vfs.inode_path_map[inode]
			f = self.fetchFile(inode)

			# File is in Cache now
			fd = os.open(f, flags)
			attr = FileInfo.getattr(f)
			attr.st_ino = inode
			info.entry = attr
		except KeyError:
			log.error(f"({inode}, {hex(flags)})")
			raise FUSEError(errno.ENOENT)
		except OSError as exc:
			raise FUSEError(exc.errno)

		self.update_refs(fd, inode)

		return pyfuse3.FileInfo(fh=fd)

	async def create(self, inode_p: int, name: str, mode: int, flags: int,
					 ctx: pyfuse3.RequestContext) -> (pyfuse3.FileInfo, pyfuse3.EntryAttributes):
		inode_p = self[inode_p]
		cpath: str = os.path.join(self.vfs.cpath(inode_p), fsdecode(name))
		log.debug(f'{Col(cpath)} in {Col(inode_p)}')

		try:
			fd: int = os.open(cpath, flags | os.O_CREAT | os.O_TRUNC)
		except OSError as exc:
			raise FUSEError(exc.errno)

		attr = FileInfo.getattr(fd=fd)
		attr.st_ino = self.disk.track(cpath.__str__())
		self.vfs.add_Child(inode_p, attr.st_ino, cpath, attr)

		self.update_refs(fd, attr.st_ino)

		self.journal.log_create(attr.st_ino, cpath, flags | os.O_CREAT | os.O_TRUNC)

		# TODO: check if the same
		f: pyfuse3.FileInfo = pyfuse3.FileInfo(fh=fd)
		return f, attr

	async def unlink(self, inode_p: int, name: str, ctx: pyfuse3.RequestContext) -> None:
		inode_p = self[inode_p]
		name = fsdecode(name)
		log.debug(f'{Col(name)} in {Col(inode_p)}')
		parent = self.vfs.cpath(inode_p)
		path = os.path.join(parent, name)
		try:
			if os.path.exists(path):  # file exists in cache
				os.unlink(path)
			inode = self.disk.path_to_ino(path)
		except OSError as exc:
			raise FUSEError(exc.errno)

		# inode from /tmp might not be present here anymore but file isn't deleted in src
		info_p: DirInfo = cast(DirInfo, self.vfs.inode_path_map[inode_p])
		assert isinstance(info_p, DirInfo), "Type mismatch"
		assert inode in info_p.children, f"{inode} not in {info_p.children}, path {path}"
		info_p.children.remove(inode)
		self.disk.untrack(path)

		self.journal.log_unlink(inode_p, inode, path)
		if self.vfs.inLookupCnt(inode):
			self._forget_path(inode, path)

	async def read(self, fd: int, offset: int, length: int) -> bytes:
		try:
			os.lseek(fd, offset, os.SEEK_SET)
			return os.read(fd, length)
		except OSError as exc:
			raise FUSEError(exc.errno)

	def __fsync_with_remote(self, cache: Path, flags: int, write_ops: list[tuple[int, int]]) -> None:
		"""Should work with newly created files too as we are re-using the flags"""
		remote = self.disk.trans.toSrc(cache)
		fd_cache, fd_remote = os.open(cache, flags), os.open(remote, flags)
		for offset, buflen in write_ops:
			os.lseek(fd_cache, offset, os.SEEK_SET)
			os.lseek(fd_remote, offset, os.SEEK_SET)
			buf = os.read(fd_cache, buflen)
			os.write(fd_remote, buf)
		os.fsync(fd_remote)
		os.close(fd_cache)
		os.close(fd_remote)

	async def write(self, fd: int, offset: int, buf: bytes) -> int:
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
			# replay the write_ops without knowning fd<->inode relation,
			# so we use inodes instead of fds ...
			inode: Optional[int] = self.vfs._fd_inode_map.get(fd)
			assert inode is not None
			self.journal.log_write(inode, offset, bytes_written)
			return bytes_written
		except OSError as exc:
			raise FUSEError(exc.errno)

	# trunacte is not a function in pyfuse3

	async def release(self, fd: int) -> None:
		if self.vfs._fd_open_count[fd] > 1:
			self.vfs._fd_open_count[fd] -= 1
			return

		del self.vfs._fd_open_count[fd]
		inode = self.vfs._fd_inode_map[fd]
		del self.vfs._inode_fd_map[inode]
		del self.vfs._fd_inode_map[fd]
		log.debug(f"fd: {fd} ino: {inode}")
		try:
			os.close(fd)
		except OSError as exc:
			raise FUSEError(exc.errno)

	# extra methods
	# =============

	async def flush(self, fh: int) -> None:
		inode: Optional[int] = self.vfs._fd_inode_map.get(fh)
		assert inode is not None
		self.journal.log_flush(inode, fh)  # store write history for later sync
		return os.fsync(fh)  # data is only written to cache_dir

	async def fsync(self, fh: int, datasync: bool) -> None:
		log.warning(f' Not implemented')
		raise FUSEError(errno.ENOSYS)


class NodeOps(BasicOps):
	def printAllInodes(self, inode_p=Disk.ROOT_INODE) -> None:
		item = self.vfs.inode_path_map[inode_p]
		if isinstance(item, DirInfo):
			log.debug(f"\t\t {inode_p}: {item.children}")
			for ino in item.children:
				self.printAllInodes(ino)

	async def statfs(self, ctx: pyfuse3.RequestContext) -> pyfuse3.StatvfsData:
		"""Easisest function to get an entrypoint into the code (not many df calls all around)"""
		root_ino = DiskBase.ROOT_INODE
		self.printAllInodes()
		root = self.vfs.cpath(root_ino)
		stat_ = pyfuse3.StatvfsData()
		try:
			statfs = os.statvfs(root)
		except OSError as exc:
			raise FUSEError(exc.errno)
		for attr in ('f_bsize', 'f_frsize', 'f_blocks', 'f_bfree', 'f_bavail',
					 'f_files', 'f_ffree', 'f_favail'):
			setattr(stat_, attr, getattr(statfs, attr))

		stat_.f_namemax = statfs.f_namemax - (len(root.__str__()) + 1)
		log.info(f"elements in RAM: {Col.path(len(self.vfs.inode_path_map))}")

		if not self.journal.isCompletelyClean():
			log.info(self.disk.getSummary())
		self.journal.flushCompleteJournal()

		# modify size, used, avail
		return self.disk.statvfs(stat_)

	async def mknod(self, inode_p: int, name: str, mode: int, rdev: int,
					ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
		# create special or ordinary file
		# mostly used for fifo / pipes but nowadays mkfifo would be better suited for that
		# mostly rare use cases
		path = os.path.join(self.vfs.cpath(inode_p), fsdecode(name))
		log.info(f"{inode_p} {path} mode: {mode}, rdev: {rdev}")
		try:
			os.mknod(path, mode=(mode & ~ctx.umask), device=rdev)
			os.chown(path, ctx.uid, ctx.gid)
		except OSError as exc:
			raise FUSEError(exc.errno)
		attr = FileInfo.getattr(path=path)
		attr.st_ino = self.disk.path_to_ino(path)
		self.vfs.add_path(attr.st_ino, path)
		return attr
