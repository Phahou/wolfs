#!/usr/bin/python

# suppress 'unused' warnings
from IPython import embed

from fileInfo import FileInfo

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
from util import is_type
import util


########################################################################################################################

class VFS:
	# I need to save all os operations in this so if a os.lstat is called I can pretend I actually know the stuff
	def __init__(self, sourceDir: Path, cacheDir: Path):
		sourceDir, cacheDir = Path(sourceDir), Path(cacheDir)
		srcInfo = FileInfo(sourceDir, cacheDir, FileInfo.getattr(sourceDir))

		# TODO: make btree out of this datatype with metafile stored somewhere

		self._inode_path_map: [int, FileInfo] = {}

		# inode related:
		self._lookup_cnt = defaultdict(lambda: 0)
		self._fd_inode_map = dict()
		self._inode_fd_map = dict()
		self._fd_open_count = dict()
		self._inode_dirty_map = dict()

		# shorthands
		self.__toCachePath = lambda x: CachePath.toCachePath(sourceDir, cacheDir, x)
		self.__toSrcPath = lambda x: CachePath.toSrcPath(sourceDir, cacheDir, x)

		# root inode needs to be filled here already
		child_inodes = []
		for f in os.listdir(srcInfo.src):
			child_inodes.append(FileInfo.getattr(srcInfo.src.joinpath(Path(f))).st_ino)
		self.add_Directory(pyfuse3.ROOT_INODE, sourceDir.__str__(), FileInfo.getattr(sourceDir),
						   child_inodes=child_inodes)

	def already_open(self, inode: int):
		return inode in self._inode_fd_map

	def getRamUsage(self):
		return util.formatByteSize(util.sizeof(self._inode_path_map))

	# "properties"
	def del_inode(self, inode: int):
		del self._inode_path_map[inode]

	def set_inode_path(self, inode: int, path: str):
		path = Path(path)
		self._inode_path_map[inode].src = self.__toSrcPath(path)
		self._inode_path_map[inode].cache = self.__toCachePath(path)

	def set_inode_entry(self, inode: int, entry: pyfuse3.EntryAttributes):
		self._inode_path_map[inode].entry = entry

	# ==============
	# inode handling
	# ==============

	def inode_to_path(self, inode: int) -> Path:
		"""
		simply maps inodes to paths
		raises errno.ENOENT if not in map -> no such file or directory
		"""
		# check cache if not in cache raise error as we indexed everything from sourceDir in __init__
		# availability is a different matter we simply check if the file exists at all or not
		try:
			val = self._inode_path_map[inode].cache
		except KeyError:
			# if a file isnt existing we would have a FileInfo entry in the _inode_path_map
			print(f"inode: {inode} has not path defined: {self._inode_path_map.get(inode)}")
			raise FUSEError(errno.ENOENT)  # no such file or directory

		if isinstance(val, set):
			# In case of hardlinks, pick any path
			val = next(iter(val))
		# log.debug(Col.bg(f'_inode_to_path: {inode} -> {val}'))
		return Path(val)

	def add_Directory(self, inode: int, path: str, entry: pyfuse3.EntryAttributes, child_inodes: [int]):
		assert inode not in child_inodes
		assert Path(path).is_dir()

		self._lookup_cnt[entry.st_ino] += 1
		src_p, cache_p = self.__toSrcPath(path), self.__toCachePath(path)

		directory = FileInfo(src_p, cache_p, entry, child_inodes=child_inodes)
		self._inode_path_map[inode] = directory

	def get_FileInfo(self, inode):
		return self._inode_path_map[inode]

	def add_path(self, inode: int, path: str, file_attrs=pyfuse3.EntryAttributes()):
		self._lookup_cnt[inode] += 1
		src_p, cache_p = self.__toSrcPath(path), self.__toCachePath(path)

		# With hardlinks, one inode may map to multiple paths.
		if inode not in self._inode_path_map:
			self._inode_path_map[inode] = FileInfo(src_p, cache_p, file_attrs)
			return

		# no hardlinks for directories
		if Path(path).is_dir():
			print(path)
			assert not Path(path).is_dir()

		# generate hardlink from path as inode is already in map
		info = self._inode_path_map[inode]
		if is_type(set, [info.src, info.cache]):
			# saving both to be able to sync later to srcDir
			info.src.add(src_p)
			info.cache.add(cache_p)
		elif info.src != src_p and info.cache != cache_p:
			self._inode_path_map[inode].src = {src_p, info.src}
			self._inode_path_map[inode].cache = {path, info.cache}

	# ============
	# attr methods
	# ============

	async def setattr(self, inode, attr, fields, fh, ctx):
		# We use the f* functions if possible so that we can handle
		# a setattr() call for an inode without associated directory
		# handle.
		if fh is None:
			path_or_fh = self.inode_to_path(inode)
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
			return FileInfo.getattr(fd=self._inode_fd_map[inode])
		else:
			return FileInfo.getattr(path=self.inode_to_path(inode))

	# pyfuse3 specific ?
	# ==================

	def inLookupCnt(self, inode):
		return inode in self._lookup_cnt

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
				self.del_inode(inode)
			except KeyError:  # may have been deleted
				pass
