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
from src.libwolfs.translator import PathTranslator, MountFSDirectoryInfo
from src.libwolfs.util import Col, CallStackAware, sizeof, formatByteSize
from typing import Union, cast
from src.libwolfs.util import Path_str
from src.libwolfs.fileInfo import FileInfo, DirInfo
from src.libwolfs.errors import SOFTLINK_DISABLED_ERROR, HARDLINK_DIR_ILLEGAL_ERROR
from src.libwolfs.disk import Disk


########################################################################################################################

class VFS(PathTranslator, CallStackAware):
	# I need to save all os operations in this so if os.lstat is called I can pretend I actually know the stuff
	def __init__(self, mount_info: MountFSDirectoryInfo):
		super().__init__(mount_info)

		# TODO: make btree out of this datatype with metafile stored somewhere
		# TODO: check if getattr of cacheDir should be the same as disks statvfs as it's the root dir
		root_info: DirInfo = DirInfo(DirInfo.getattr(mount_info.cacheDir), [])
		self.inode_path_map: dict[int, Union[FileInfo, DirInfo]] = {Disk.ROOT_INODE: root_info}

		# inode related: (used for memory management)
		self._lookup_cnt: dict[int, int] = defaultdict(lambda: 0)  # reference counter for pyfuse3
		self._inode_fd_map: dict[int, int] = dict()  # maps inodes to file descriptors

		# unused by this class only declared here
		self._fd_inode_map: dict[int, int] = dict()  # maps file descriptors to inodes
		self._fd_open_count: dict[int, int] = dict()  # reference counter if inode is still open (being used)

	# "properties"
	def del_inode(self, inode: int) -> None:
		# todo: needs to hold information that remote has to be deleted too
		# TODO: also change info of parent inode
		del self.inode_path_map[inode]

	# def set_inode_entry(self, inode: int, entry: pyfuse3.EntryAttributes) -> None:
	#	self.inode_path_map[inode].entry = entry

	# inode <-> path funcs
	# ====================

	def add_Child(self,
		inode_p: int,
		inode: int,
		path: str,
		entry: pyfuse3.EntryAttributes) -> None:
		"""Also adds file to parent inode `inode_p`"""
		assert inode_p != inode, f"{self} inode_p({Col(inode_p)}) can't be inode({Col(inode)})"
		assert inode == entry.st_ino, 'entry ino must be the same as lookup ino'
		self.add_path(inode, path, entry)

		info_p = self.inode_path_map[inode_p]
		assert isinstance(info_p, DirInfo), f"Logical error? {info_p} has to be DirInfo not {type(info_p)}"
		if inode not in info_p.children:
			info_p.children.append(inode)

	def _add_Directory(self,
		inode_p: int,
		wolfs_inode: int,
		inode_path: str,
		entry: pyfuse3.EntryAttributes) -> DirInfo:
		directory = DirInfo(entry, [])

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

		return directory

	def add_path(self,
		inode: int,
		path: str,
		file_attrs: pyfuse3.EntryAttributes) -> None:
		"""
		Add associated (ino, path)-metadata and increase their lookup count

		:param inode: inode to be added and increased
		:param path: path associated with inode
		:param file_attrs: metadata of inode
		"""
		# TODO:
		#  -> Rewrite so that we only serve file Attributes and no paths anymore as
		#     translator is better suited for this
		#     => this rework should simplify most of this class
		assert inode == file_attrs.st_ino, f'inode and file_attrs.st_ino have to be the same as file_attrs.st_ino are for lookup'
		self._lookup_cnt[inode] += 1

		# With hardlinks, one inode may map to multiple paths.
		if inode not in self.inode_path_map:
			self.inode_path_map[inode] = FileInfo(file_attrs)
			return

		# no hardlinks for directories
		assert not os.path.isdir(path), f"Programming Error: {self}{HARDLINK_DIR_ILLEGAL_ERROR} | (path: {path}) use add_Directory()"

	# pyfuse3 specific
	# ================

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
