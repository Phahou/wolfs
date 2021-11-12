#!/usr/bin/python

# suppress 'unused' warnings
from IPython import embed

embed = embed

import os
import pyfuse3
import errno
import stat as stat_m
from pyfuse3 import FUSEError
from collections import defaultdict
from pathlib import Path
import logging
log = logging.getLogger(__name__)
from disk import CachePath
from util import Col, CallStackAware, sizeof, formatByteSize
from typing import Union, Final, cast
from fileInfo import FileInfo, DirInfo
from errors import SOFTLINK_DISABLED_ERROR, HARDLINK_DIR_ILLEGAL_ERROR


########################################################################################################################

class VFS(CallStackAware):
	__next_inode: int = pyfuse3.ROOT_INODE

	def __inode_generator(self) -> int:
		inode = self.__next_inode
		self.__next_inode += 1
		return inode

	# I need to save all os operations in this so if a os.lstat is called I can pretend I actually know the stuff
	def __init__(self, sourceDir: Path, cacheDir: Path):
		self.sourceDir: Final[Path] = Path(sourceDir)
		self.cacheDir: Final[Path] = Path(cacheDir)

		# TODO: make btree out of this datatype with metafile stored somewhere
		self.inode_path_map: dict[int, Union[FileInfo, DirInfo]] = dict()

		# inode related:
		self._lookup_cnt: dict[int, int] = defaultdict(lambda: 0)  # reference counter for pyfuse3
		self._fd_inode_map: dict[int, int] = dict()  # maps file descriptors to inodes
		self._inode_fd_map: dict[int, int] = dict()  # maps inodes to file descriptors
		self._fd_open_count: dict[int, int] = dict()  # reference counter if inode is still open (being used)

	# shorthands
	def __toCachePath(self, path: Union[str, Path]) -> Path:
		return CachePath.toDestPath(self.sourceDir, self.cacheDir, path)

	def __toSrcPath(self, path: Union[str, Path]) -> Path:
		return CachePath.toDestPath(self.cacheDir, self.sourceDir, path)

	def already_open(self, inode: int) -> bool:
		return inode in self._inode_fd_map

	def getRamUsage(self) -> str:
		return formatByteSize(sizeof(self.inode_path_map))

	# "properties"
	def del_inode(self, inode: int) -> None:
		# todo: needs to hold information that remote has to be deleted too
		# TODO: also change info of parent inode
		del self.inode_path_map[inode]

	def set_inode_path(self, inode: int, path: Union[str, Path]) -> None:
		self.inode_path_map[inode].src = self.__toSrcPath(path)
		self.inode_path_map[inode].cache = self.__toCachePath(path)

	def set_inode_entry(self, inode: int, entry: pyfuse3.EntryAttributes) -> None:
		self.inode_path_map[inode].entry = entry

	# ==============
	# inode handling
	# ==============

	def inode_to_cpath(self, inode: int) -> Path:
		"""Maps inodes to paths. Might raise `FUSEError(errno.ENOENT)`"""
		try:
			val = self.inode_path_map[inode].cache
		except KeyError:  # file likely doesnt exist. Logic error otherwise
			log.error(f'inode: {Col(inode)} has no path defined')
			raise FUSEError(errno.ENOENT)

		if isinstance(val, set):
			val = next(iter(val))  # In case of hardlinks, pick any path
		return val

	def get_FileInfo(self, inode: int) -> FileInfo:
		return self.inode_path_map[inode]

	# inode <-> path funcs
	# ====================

	# search for an inode via path
	def getInodeOf(self, path: str, inode_p: int) -> int:
		"""Get Inoder referencing ´path´ which is in directory inode ´inode_p´"""
		i2p = self.inode_to_cpath
		info: Union[FileInfo, DirInfo] = self.inode_path_map[inode_p]
		if isinstance(info, DirInfo):
			children: list[int] = cast(DirInfo, self.inode_path_map[inode_p]).children
		else:
			assert isinstance(info, FileInfo), f"Type mismatch got: {type(info)}, expected FileInfo"
			return 0
		assert children, f'children is empty {self.inode_path_map[inode_p].__str__()}'
		paths: list[tuple[int, str]] = [(child_inode, i2p(child_inode).__str__()) for child_inode in children]
		old_inode: list[tuple[int, str]] = list(filter(lambda path_: path_[1] == path, paths))
		assert len(old_inode) < 2, f'We have 2 full_paths?'
		if old_inode:
			return old_inode[0][0]
		else:
			# inode doesnt exist yet (not found)
			return 0

	def addFilePath(self, inode_p: int, inode: int, path: str, entry: pyfuse3.EntryAttributes) -> None:
		"""Also adds file to parent inode `inode_p`"""
		assert inode_p != inode, f"{self} inode_p({Col(inode_p)}) can't be inode({Col(inode)})"
		assert inode == entry.st_ino, 'entry ino must be the same as lookup ino'
		self.add_path(inode, path, entry)

		info_p = self.inode_path_map[inode_p]
		assert isinstance(info_p, DirInfo), f"Logical error? {info_p} has to be DirInfo not {type(info_p)}"
		if inode not in info_p.children:
			info_p.children.append(inode)

	def add_Directory(self, inode: int, path: str, entry: pyfuse3.EntryAttributes, child_inodes: list[int]) -> DirInfo:
		# TODO: needs rework to also include inode_p
		assert inode not in child_inodes
		assert entry.st_ino == inode
		assert Path(path).is_dir()

		self._lookup_cnt[inode] += 1
		src_p, cache_p = self.__toSrcPath(path), self.__toCachePath(path)

		directory = DirInfo(src_p, cache_p, entry, child_inodes=child_inodes)
		self.inode_path_map[inode] = directory
		return directory

	def add_path(self, inode: int, path: str, file_attrs: pyfuse3.EntryAttributes = pyfuse3.EntryAttributes()) -> None:
		assert inode == file_attrs.st_ino, f'inode and file_attrs.st_ino have to be the same as file_attrs.st_ino are for lookup'
		self._lookup_cnt[inode] += 1
		src_p, cache_p = self.__toSrcPath(path), self.__toCachePath(path)

		# With hardlinks, one inode may map to multiple paths.
		if inode not in self.inode_path_map:
			self.inode_path_map[inode] = FileInfo(src_p, cache_p, file_attrs)
			return

		# no hardlinks for directories
		assert not os.path.isdir(path), f"{self}{HARDLINK_DIR_ILLEGAL_ERROR} | (path: {path})"

		# generate hardlink from path as inode is already in map
		info = self.inode_path_map[inode]
		# we only need to check one entry as both are always the same type
		if isinstance(info.src, set):
			assert isinstance(info.cache, set), f"{self} cache & src should always be the same type!"
			# saving both to be able to sync later to srcDir
			info.src.add(src_p)
			info.cache.add(cache_p)
		elif info.src != src_p:
			assert False, f"{self}{SOFTLINK_DISABLED_ERROR}"
			self.inode_path_map[inode].src = cast(set, {src_p, info.src})
			self.inode_path_map[inode].cache = cast(set, {cache_p, info.cache})

	# ============
	# attr methods
	# ============

	async def setattr(self, inode: int, attr: pyfuse3.EntryAttributes, fields: pyfuse3.SetattrFields, fh: int, ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
		if fh is None:
			path_or_fh = self.inode_to_cpath(inode)
		else:
			path_or_fh = fh
		FileInfo.setattr(attr, fields, path_or_fh, ctx)

		return await self.getattr(inode)

	async def getattr(self, inode: int, ctx: pyfuse3.RequestContext = None) -> pyfuse3.EntryAttributes:
		if self.already_open(inode):  # if isOpened(inode):
			return FileInfo.getattr(fd=self._inode_fd_map[inode])
		else:
			return FileInfo.getattr(path=self.inode_to_cpath(inode))

	# pyfuse3 specific ?
	# ==================

	def inLookupCnt(self, inode: int) -> bool:
		return inode in self._lookup_cnt

	# inode functions
	async def forget(self, inode_list: list[tuple[int, int]]) -> None:
		for (inode, nlookup) in inode_list:
			if self._lookup_cnt[inode] > nlookup:
				self._lookup_cnt[inode] -= nlookup
				continue
			log.debug(f'{Col.BY}forgetting about inode {Col(inode)}')
			assert inode not in self._inode_fd_map
			del self._lookup_cnt[inode]
		# try:
		#	self.del_inode(inode)
		# except KeyError:  # may have been deleted
		#	log.warning(self + f' already deleted {Col.bg(inode)}')
