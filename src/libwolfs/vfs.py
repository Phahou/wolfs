#!/usr/bin/python
# job of this module:
#  - add/delete paths (directories/files) to internal data structures

# suppress 'unused' warnings
from IPython import embed

embed = embed

import os
import pyfuse3
import errno
from pyfuse3 import FUSEError
from collections import defaultdict
from pathlib import Path
import logging
log = logging.getLogger(__name__)
from src.libwolfs.translator import PathTranslator
from src.libwolfs.util import Col, CallStackAware, sizeof, formatByteSize
from typing import Union, cast
from src.libwolfs.fileInfo import FileInfo, DirInfo
from src.libwolfs.errors import SOFTLINK_DISABLED_ERROR, HARDLINK_DIR_ILLEGAL_ERROR


########################################################################################################################

class VFS(PathTranslator, CallStackAware):
	# I need to save all os operations in this so if os.lstat is called I can pretend I actually know the stuff
	def __init__(self, sourceDir: Path, cacheDir: Path):
		super().__init__(sourceDir, cacheDir)
		#self.sourceDir: Final[Path] = Path(sourceDir)
		#self.cacheDir: Final[Path] = Path(cacheDir)

		# TODO: make btree out of this datatype with metafile stored somewhere
		self.inode_path_map: dict[int, Union[FileInfo, DirInfo]] = dict()

		# inode related: (used for memory management)
		self._lookup_cnt: dict[int, int] = defaultdict(lambda: 0)  # reference counter for pyfuse3
		self._inode_fd_map: dict[int, int] = dict()  # maps inodes to file descriptors

		# unused by this class only declared here
		self._fd_inode_map: dict[int, int] = dict()  # maps file descriptors to inodes
		self._fd_open_count: dict[int, int] = dict()  # reference counter if inode is still open (being used)

	def already_open(self, inode: int) -> bool:
		return inode in self._inode_fd_map

	# "properties"
	def del_inode(self, inode: int) -> None:
		# todo: needs to hold information that remote has to be deleted too
		# TODO: also change info of parent inode
		del self.inode_path_map[inode]

	def set_inode_path(self, inode: int, path: Union[str, Path]) -> None:
		self.inode_path_map[inode].src = self.toSrc(path)
		self.inode_path_map[inode].cache = self.toTmp(path)

	# def set_inode_entry(self, inode: int, entry: pyfuse3.EntryAttributes) -> None:
	#	self.inode_path_map[inode].entry = entry

	# ==============
	# inode handling
	# ==============

	def cpath(self, inode: int) -> Path:
		"""Maps inodes to paths. Might raise `FUSEError(errno.ENOENT)`"""
		try:
			val = self.inode_path_map[inode].cache
		except KeyError:  # file likely doesnt exist. Logic error otherwise
			log.error(f'inode: {Col(inode)} has no path defined')
			raise FUSEError(errno.ENOENT)

		if isinstance(val, set):
			val = next(iter(val))  # In case of hardlinks, pick any path
		return val

	# inode <-> path funcs
	# ====================

	def add_Child(self, inode_p: int, inode: int, path: str, entry: pyfuse3.EntryAttributes) -> None:
		"""Also adds file to parent inode `inode_p`"""
		assert inode_p != inode, f"{self} inode_p({Col(inode_p)}) can't be inode({Col(inode)})"
		assert inode == entry.st_ino, 'entry ino must be the same as lookup ino'
		self.add_path(inode, path, entry)

		info_p = self.inode_path_map[inode_p]
		assert isinstance(info_p, DirInfo), f"Logical error? {info_p} has to be DirInfo not {type(info_p)}"
		if inode not in info_p.children:
			info_p.children.append(inode)

	def _add_Directory(self, inode_p: int, wolfs_inode: int, inode_path: str, entry: pyfuse3.EntryAttributes) -> DirInfo:
		src_p, cache_p = self.toSrc(inode_path), self.toTmp(inode_path)
		directory = DirInfo(src_p, cache_p, entry, [])

		# "create" directory in inode table
		self.inode_path_map[wolfs_inode] = directory
		self._lookup_cnt[wolfs_inode] += 1

		# update parent accordingly
		if parent := cast(DirInfo, self.inode_path_map.get(inode_p)):
			if wolfs_inode in parent.children:
				pass # optimize to return early
			else:
				parent.children.append(wolfs_inode)

		# post-condition:
		assert directory == self.inode_path_map[wolfs_inode]
		assert wolfs_inode in parent.children
		# TODO: merge vfs and disk translators into disk or make a new one called FSTranslator
		# assert inode_path != self.disk.trans.getParent()

		return directory

	def add_path(self, inode: int, path: str, file_attrs: pyfuse3.EntryAttributes = pyfuse3.EntryAttributes()) -> None:
		assert inode == file_attrs.st_ino, f'inode and file_attrs.st_ino have to be the same as file_attrs.st_ino are for lookup'
		self._lookup_cnt[inode] += 1
		src_p, cache_p = self.toSrc(path), self.toTmp(path)

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
			path_or_fh = self.cpath(inode)
		else:
			path_or_fh = fh
		FileInfo.setattr(attr, fields, path_or_fh, ctx)
		# todo check if attr now is attr after self.getattr
		new_attr = self.getattr(inode)
		assert attr != new_attr, "attr are equal ?"
		return await new_attr

	async def getattr(self, inode: int, ctx: pyfuse3.RequestContext = None) -> pyfuse3.EntryAttributes:
		if self.already_open(inode):  # if isOpened(inode):
			return FileInfo.getattr(fd=self._inode_fd_map[inode])
		else:
			return FileInfo.getattr(path=self.cpath(inode))

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
