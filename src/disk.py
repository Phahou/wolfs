#!/usr/bin/python

# suppress 'unused' warnings
import typing

from IPython import embed

embed = embed

from pathlib import Path
import shutil
import errno
import sys
import os
from util import Col, formatByteSize
import time
import logging
import pyfuse3
from pyfuse3 import FUSEError
from fileInfo import FileInfo
from util import __functionName__
from errors import NotEnoughSpaceError
from typing import Union
from sortedcontainers import SortedDict
import functools
from typing import Final

log = logging.getLogger(__name__)
Path_str = Union[str, Path]


class CachePath(Path):
	@staticmethod
	def toSrcPath(sourceDir: Path, cacheDir: Path, path: Path_str) -> Path:
		_path: str = path if isinstance(path, str) else path.__str__()
		return Path(_path.replace(cacheDir.__str__(), sourceDir.__str__()))

	@staticmethod
	def toCachePath(sourceDir: Path, cacheDir: Path, path: Path_str) -> Path:
		_path: str = path if isinstance(path, str) else path.__str__()
		return Path(_path.replace(sourceDir.__str__(), cacheDir.__str__()))


class Disk:
	__MEGABYTE__: Final[int] = 1024 * 1024
	__NANOSEC_PER_SEC__: Final[int] = 1_000_000_000
	ROOT_INODE: Final[int] = 2

	def __init__(self, sourceDir: Path, cacheDir: Path, maxCacheSize: int, noatime: bool, cacheThreshold: float = 0.99):
		# fs related:
		self.sourceDir = Path(sourceDir)
		if not self.sourceDir.exists():
			print(f'[Errno {errno.ENOENT}] {os.strerror(errno.ENOENT)}: {sourceDir}')
			sys.exit(errno.ENOENT)

		self.cacheDir = Path(cacheDir)
		if not self.cacheDir.exists():
			print(f'[Errno {errno.ENOENT}] {os.strerror(errno.ENOENT)}: {cacheDir}')
			sys.exit(errno.ENOENT)

		# cache related:
		self.__current_CacheSize: int = 0
		self.__cacheThreshold: float = cacheThreshold
		self.__maxCacheSize: int = maxCacheSize * self.__MEGABYTE__
		self.time_attr: str = 'st_mtime_ns' if noatime else 'st_atime_ns'  # remote has mountopt noatime set?

		self.in_cache: SortedDict[int, (str, int)] = SortedDict()
		self.path_timestamp: dict[str, int] = dict()
		self.__cached_inos: dict[int, bool] = dict()

		# for mnt_ino_translation and path_to_ino mapping functions
		self.__mnt_ino2st_ino: dict[int, int] = {pyfuse3.ROOT_INODE: self.ROOT_INODE, self.ROOT_INODE: self.ROOT_INODE}
		self.__tmp_ino2st_ino: dict[int, int] = {pyfuse3.ROOT_INODE: self.ROOT_INODE, self.ROOT_INODE: self.ROOT_INODE}
		self.__last_ino: int = 1  # as the first ino is always 2 (ino 1 is for bad blocks but fuse doesnt act that way)
		self.__freed_inos: list[int] = []
		self.path_ino_map: dict[str, int] = dict()

	def getNumberOfElements(self) -> int:
		return len(self.in_cache)

	# ===========
	# private api
	# ===========

	def isInCache(self, ino: int) -> bool:
		return ino in self.__cached_inos

	def _rebuildCacheDir(self) -> None:
		"""
		Rebuild the internal book-keeping ( `self.__curent_CacheSize` , `self.in_cache` ) variables\n
		based on the contents currently in `self.cacheDir`
		"""
		self.in_cache.clear()
		self.__current_CacheSize = 0
		getsize, islink, join = os.path.getsize, os.path.islink, os.path.join
		for dirpath, dirnames, filenames in os.walk(self.cacheDir, followlinks=False):
			for f in filenames:
				path = join(dirpath, f)
				# skip symbolic links for now
				if islink(path):
					continue
				self.cp2Cache(Path(path))

	# ==========
	# public api
	# ==========

	def toCachePath(self, path: Path_str) -> Path:
		return CachePath.toCachePath(self.sourceDir, self.cacheDir, path)

	def toSrcPath(self, path: Path_str) -> Path:
		return CachePath.toSrcPath(self.sourceDir, self.cacheDir, path)

	def toRootPath(self, path: Path_str) -> str:
		"""Get the Path without the cache or src prefix"""
		root: str = self.toSrcPath(path).__str__().replace(self.sourceDir.__str__(), '')
		return root if root else '/'

	def mnt_ino_translation(self, inode: int) -> int:
		return inode if inode != pyfuse3.ROOT_INODE else self.ROOT_INODE

	def path_to_ino(self, some_path: Path_str, reuse_ino=0) -> int:
		"""
		Maps paths to inodes. Creates them if necessary
		re-uses ino `reuse_ino` if != 0
		"""
		path: str = self.toRootPath(some_path)

		if ino := self.path_ino_map.get(path):
			ino = ino
		# completely unused currently
		#elif self.__freed_inos:
		#	ino = self.__freed_inos[0]
		#	del self.__freed_inos[0]
		#	print(f"{__functionName__(self)} ino from deleted self.__freed_inos[0]:{ino}")
		elif reuse_ino != 0:  # for rename operations
			ino = reuse_ino
		else:
			ino = self.__last_ino + 1
			self.__last_ino += 1
		self.path_ino_map[path] = ino

		if os.path.exists(some_path):  # update foreign translation only if accessible
			foreign_ino: int = os.stat(some_path).st_ino
			if some_path.__str__().startswith(self.sourceDir.__str__()):
				self.__mnt_ino2st_ino[foreign_ino] = ino
			else:
				self.__tmp_ino2st_ino[foreign_ino] = ino
		return ino

	@staticmethod
	def cpdir(src: Path, dst: Path, added_folders: list[Path] = [], added_size: int = 0) -> tuple[int, list[Path]]:
		"""creates a dir  `dst` and all its missing parents. Copies attrs of `src` and its parents accordingly"""
		if not dst.exists():
			if not dst.parent.exists():
				tmp_size, added_folders = Disk.cpdir(src.parent, dst.parent, added_folders + [src.parent])
				added_size += tmp_size
			stat_result = os.stat(src)
			mode = stat_result.st_mode
			added_size += stat_result.st_size
			dst.mkdir(mode=mode, parents=False)
		shutil.copystat(src, dst)
		return added_size, added_folders

	@staticmethod
	def getSize(path: str = '.') -> int:
		""":returns: Size of path. Skips symbolic links"""
		total_size = 0
		for dirpath, dirnames, filenames in os.walk(path):
			for f in filenames:
				fp = os.path.join(dirpath, f)
				# skip if it is symbolic link
				if not os.path.islink(fp):
					total_size += os.path.getsize(fp)
		return total_size

	def canFit(self, size: int) -> bool:
		"""Is the cache large enough to hold `size` ?"""
		# if isinstance(file, FileInfo):
		return size < self.__maxCacheSize

	def canStore(self, path: Path) -> bool:
		return self.__canStore(path)

	def __canStore(self, path: Union[FileInfo, Path, str]) -> bool:
		"""
		Does the cache have room  for `path` ?
		Cache size needs to be updated if file is inserted into cacheDir.
		:param path: returns False on symbolic links
		"""
		if isinstance(path, FileInfo):
			assert not isinstance(path.cache, set), f"{__functionName__(self)} sets(softlinks) are currently not implemented{Col.END}"
			store_able = not os.path.islink(path.cache)\
						 and ((path.entry.st_size + self.__current_CacheSize) < self.__maxCacheSize)
		elif isinstance(path, Path) or isinstance(path, str):
			# TODO: maybe try to get filesize via lstat (like stat but supports links)
			p = path.__str__() if isinstance(path, Path) else path
			store_able = not os.path.islink(p) and os.path.getsize(p) + self.__current_CacheSize < self.__maxCacheSize
		else:
			store_able = ""
			assert f'{__functionName__(self)}: Types are wrong: {path}({type(path)}) not in [Path, str, FileInfo]'
		return store_able

	def isFilledBy(self, percent: float) -> bool:
		""":param percent: between [0.0, 1.0]"""
		assert 0.0 < percent < 1.0, 'disk_isFullBy: needs to be [0-1]'
		diskUsage = self.__current_CacheSize / self.__maxCacheSize
		return True if diskUsage >= percent else False

	def isFull(self, use_threshold: bool = False) -> bool:
		if use_threshold:
			return self.isFilledBy(self.__cacheThreshold)
		return self.isFilledBy(1.0)

	def untrack(self, path: str) -> None:
		"""Doesnt track `path` anymore and frees up its reserved size."""
		src_path: str = self.toSrcPath(path).__str__()
		if timestamp := self.path_timestamp.get(src_path):
			try:
				i: int = 0
				item: Union[tuple[str, int], list[tuple[str, int]]] = self.in_cache[timestamp]
				if isinstance(item, list):
					i = [y[0] for y in item].index(src_path)
					item = item[i]

				(t_path, size) = item[0], item[1]
				self.__current_CacheSize -= size

				if isinstance(item, list):
					del self.in_cache[timestamp][i]
					if len(self.in_cache) == 0:
						del self.in_cache[timestamp]
				else:
					del self.in_cache[timestamp]

				del self.path_timestamp[src_path]
				del self.__cached_inos[self.path_to_ino(src_path)]
			except KeyError:
				pass
				#embed()
			self.old_src_path = src_path

	def track(self, path: str, force: bool = False, reuse_ino=0) -> int:
		"""
		Add `path` to internal filing structure and reserve it's disk space
		reuse_ino: re-use an old inode
		"""

		# handling of create and mkdir (files dont exist yet so os.stat(path_) will throw an error)
		path_: str = self.toSrcPath(path).__str__()
		if not os.path.exists(path_):
			path_ = self.toCachePath(path).__str__()

		timestamp: int = getattr(os.stat(path_), self.time_attr) // self.__NANOSEC_PER_SEC__
		size: int = os.path.getsize(path_)
		src_path: str = self.toSrcPath(path).__str__()
		ino: int = self.path_to_ino(src_path, reuse_ino=reuse_ino)
		tracked_path = self.in_cache.get(timestamp)
		assert isinstance(tracked_path, list) or isinstance(tracked_path, tuple) or tracked_path is None, "Type mismatch!"
		if isinstance(self.in_cache.get(timestamp), list):
			self.in_cache[timestamp].append((src_path, size))
		elif isinstance(tracked_path, tuple):
			self.in_cache[timestamp] = [self.in_cache[timestamp]] + [(src_path, size)]
		else:
			self.in_cache[timestamp] = (src_path, size)
		self.path_timestamp[src_path] = timestamp
		self.__cached_inos[ino] = True
		self.__current_CacheSize += size
		return ino

	def get_head_in_cache(self) -> tuple[str, int]:
		item: Union[tuple[str, int], list[tuple[str, int]]] = self.in_cache.peekitem(index=0)[1]
		if isinstance(item, list):
			item = item[0]
		src_path, size = item[0], item[1]
		return src_path, size


	def __make_room_for_path(self, force: bool, path: Path, open_paths: list[Path] = None) -> None:
		if open_paths is None:
			open_paths = []
		while force and not self.__canStore(path):
			try:
				(src_path, size) = self.get_head_in_cache()
				#assert timestamp in self.path_timestamp
				self.untrack(src_path)
			except IndexError:
				log.warning(f"Deleted all non open files and still couldn't store file: {path}")
				raise FUSEError(errno.EDQUOT)

			cpath: Path = Path(self.toCachePath(src_path))

			# skip open files (we cant sync and close them as they might be written / read from)
			if cpath in open_paths:
				continue

			assert cpath.exists(), f'File {Col.path(cpath)} not in cache although it should be ?'

			if os.path.isfile(cpath):
				os.remove(cpath)
			elif os.path.isdir(cpath):
				try:
					os.rmdir(cpath)
				except OSError:
					# directory isnt empty though I dont know what to do at this point
					# as re-adding it isnt a option ( directory entries only change if files are added or deleted so)
					# furthermore it would break the heap
					# at least dont let the CacheSize get corrupted by this
					self.__current_CacheSize += size

	# todo: think about making a write cache for newly created files -> store write_ops
	#       check after a timeout if said files still exist or are still referenced if not
	#       then they were tempfiles anyway otherwise sync them to the backend
	def cp2Cache(self, path: Path, force: bool = False, open_paths: list[Path] = None) -> Path:
		"""
		:param path: file/dir to be copied
		:param force: Delete files if necessary
		:param open_paths: paths which have an open file descriptor and can't be closed as they are in use
		:raises NotEnoughSpaceError: If there isn't enough space to save `path` and `force` wasn't set
		:raises FUSEError(errno.EDQUOT): if all non open files were deleted and there still isnt enough room for `path`
		:returns: Cache path of copied file/dir
		Copy `file` and its meta-data into Cache. If `force` is set it deletes least recently used files until enough space is available.

		Note:
		  Make sure to sync the cache and remote before copying if using `force` as it could potentially delete
		  a dirty file which has not yet been sync'd and data would be lost
		  calls assert if this happens at the moment
		"""
		self.__make_room_for_path(force, path, open_paths)

		if self.__canStore(path):
			dest = self.toCachePath(path)
			addedDirsSize, addedFolders = Disk._cp2Dir(path, dest)
			self.__current_CacheSize += addedDirsSize
			# folders which are created by cp2Dir are untracked and should be tracked...
			# elements in cache doesnt model reality (too few entries too many undocumented)
			for parent in addedFolders:
				self.path_to_ino(parent)
				self.track(parent.__str__(), force)
			if addedDirsSize == 0:
				self.path_to_ino(dest)
				self.track(path.__str__(), force)

			# TODO: use xattributes later and make a custom field:
			# sth like __wolfs_atime__ : time.time_ns()
			#   for the last access of a file
			# so we retain standard-conformity

			return dest
		else:
			raise NotEnoughSpaceError('Not enough space')

	@staticmethod
	def _cp2Dir(src: Path_str, dst: Path_str) -> tuple[int, list[Path]]:
		"""
		Create a copy of `src` in `dst` while also keeping meta-data.
		Creates parent directories on the fly.
		Ignores special files. (read softlinks, device, etc. )

		:param src: original File
		:param dst: destination File to have the same attributes and contents of `src` after the call
		:returns: Path of copied file in cacheDir
		"""
		src, dst = Path(src), Path(dst)
		addedDirsSize: int = 0
		addedFolders: list[Path] = []
		if src == dst:
			# same File no need for copying,
			# file should exist already too as self.__canStore would throw errors otherwise
			# just give 0 back as we effictively wont do anything and just skip it
			return 0, []

		if src.is_dir():
			addedDirsSize, addedFolders = Disk.cpdir(src, dst)
		elif src.is_file():
			if not dst.parent.exists():
				addedDirsSize, addedFolders = Disk.cpdir(src.parent, dst.parent)
			shutil.copy2(src, dst)
		else:
			msg = f'{Col.BY} Unrecognized filetype: {src} -> ignoring'
			log.error(msg)
			raise IOError(msg)

		Disk.cpAttrs(src, dst)
		return addedDirsSize, addedFolders

	@staticmethod
	def cpAttrs(src: Path_str, dst: Path_str) -> None:
		# book-keeping of file-attributes (also takes care of parent dirs having wrong modes from previous runs)
		src_mode = os.stat(src).st_mode
		Path(dst).chmod(src_mode)
		shutil.copystat(src, dst)

	def getCurrentStatus(self) -> tuple:
		""":returns: Current (fullness in %, usedCache, maxCache) of self.cacheDir"""
		usedCache = self.__current_CacheSize
		maxCache = self.__maxCacheSize
		return 100 * usedCache / maxCache, usedCache, maxCache

	def getSummary(self) -> str:
		diskUsage, usedCache, maxCache = self.getCurrentStatus()
		diskUsage = Col.path(f'{Col.BY}{diskUsage:.8f}%')
		usedCache = Col.path(formatByteSize(usedCache))
		maxCache = Col.path(formatByteSize(maxCache))
		copySummary = Col.BW \
					  + f'Cache is currently storing {Col.inode(self.getNumberOfElements())} elements and is {diskUsage} ' \
					  + f' full (used: {usedCache} / {maxCache} )'
		return str(copySummary)

	def printSummary(self) -> None:
		print(self.getSummary())
