#!/usr/bin/env python

# suppress 'unused' warnings

from IPython import embed
embed = embed

from pathlib import Path
import shutil
import errno
import sys
import os
import logging
import pyfuse3
from pyfuse3 import FUSEError
from src.util import Col, formatByteSize
from errors import NotEnoughSpaceError, SOFTLINK_DISABLED_ERROR
from typing import Union, Final
from sortedcontainers import SortedDict

log = logging.getLogger(__name__)
Path_str = Union[str, Path]


class CachePath(Path):
	@staticmethod
	def toRootPath(sourceDir: Path, cacheDir: Path, path: Path_str) -> str:
		"""Get the Path without the cache or src prefix"""
		_path: str = path if isinstance(path, str) else path.__str__()
		root = _path.replace(sourceDir.__str__(), '').replace(cacheDir.__str__(), '')
		return ('/' + root).replace('//', '/') if root else '/'

	@staticmethod
	def toDestPath(sourceDir: Path, destDir: Path, path: Path_str) -> Path:
		root = CachePath.toRootPath(sourceDir, destDir, path)
		result = f"{destDir.__str__()}{root}".replace('//', '/')
		return Path(result)


class Disk:
	__MEGABYTE__: Final[int] = 1024 * 1024
	__NANOSEC_PER_SEC__: Final[int] = 1_000_000_000
	ROOT_INODE: Final[int] = 2
	MIN_DIR_SIZE: Final[int] # set in __init__

	def __init__(self, sourceDir: Path, cacheDir: Path, maxCacheSize: int, noatime: bool = True, cacheThreshold: float = 0.99):
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
		self.__freed_inos: set[int] = set()
		self.path_ino_map: dict[str, int] = dict()

		# get OS dependant minimum directory size
		os.mkdir('wolfs_tmp_directory')
		self.MIN_DIR_SIZE = os.stat('wolfs_tmp_directory').st_size
		os.rmdir('wolfs_tmp_directory')

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

	# Helpers
	def mnt_ino_translation(self, inode: int) -> int:
		return inode if inode != pyfuse3.ROOT_INODE else self.ROOT_INODE

	def del_inode(self, inode: int, path: str) -> None:
		self.__freed_inos.add(inode)
		rpath = self.toRootPath(path)
		del self.path_ino_map[rpath]

	def toCachePath(self, path: Path_str) -> Path:
		return CachePath.toDestPath(self.sourceDir, self.cacheDir, path)

	def toSrcPath(self, path: Path_str) -> Path:
		return CachePath.toDestPath(self.cacheDir, self.sourceDir, path)

	def toRootPath(self, path: Path_str) -> str:
		return CachePath.toRootPath(self.sourceDir, self.cacheDir, path)

	def path_to_ino(self, some_path: Path_str, reuse_ino=0) -> int:
		"""
		Maps paths to inodes. Creates them if necessary
		re-uses ino `reuse_ino` if != 0
		:raise ValueError instead of asserts on logic errors
		"""
		path: str = self.toRootPath(some_path)

		if ino := self.path_ino_map.get(path):
			ino = ino
		elif reuse_ino != 0:  # for rename operations
			if reuse_ino > self.__last_ino:
				raise ValueError(f"Reused ino is larger than largest generated ino {reuse_ino} > {self.__last_ino}")
			elif reuse_ino in self.__freed_inos:
				ino = reuse_ino
			else:
				raise ValueError(f"Reused ino {reuse_ino} is not in freed ino set {self.__freed_inos}")
		else:
			ino = self.__last_ino + 1
			self.__last_ino += 1
		self.path_ino_map[path] = ino

		if os.path.exists(some_path):  # update foreign translation only if accessible
			foreign_ino: int = os.stat(some_path).st_ino
			if some_path.__str__().startswith(self.sourceDir.__str__()):
				self.__mnt_ino2st_ino[foreign_ino] = ino
			elif some_path.__str__().startswith(self.cacheDir.__str__()):
				self.__tmp_ino2st_ino[foreign_ino] = ino
			else:
				raise ValueError(f"Wrong input! {some_path} has to have a prefix of {self.sourceDir} or {self.cacheDir}")
		return ino

	def mkdir_p(self, src: Path, added_folders=None, added_size: int = 0) -> tuple[int, list[Path]]:
		"""
		Recursively re-creates the directoriy path up until `src` for `dst`.

		Bascially the same as "mkdir -P dst" with the caviat that the attributes of the corresponding `src`-directory
		  are copied too.

		:return tuple of added empty folder sizes list of filepath to in between folders

		actually it is like generate in betweens
		"""
		dst: Path = self.toCachePath(src)

		if added_folders is None:
			added_folders = []

		if not dst.exists():
			if not dst.parent.exists():
				tmp_size, added_folders = self.mkdir_p(src.parent, added_folders)
				added_size += tmp_size

			added_folders += [src]
			stat_result = os.stat(src)
			mode = stat_result.st_mode
			added_size += stat_result.st_size
			dst.mkdir(mode=mode, parents=False)

		Disk.copystat(src, dst)
		return added_size, added_folders

	# Size related

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

	def canReserve(self, size: int) -> bool:
		return self.canFit(size + self.__current_CacheSize)

	def canStore(self, path: Path) -> bool:
		"""Does the cache have room  for `path` ?"""
		if isinstance(path, Path):
			# TODO: maybe try to get filesize via lstat (like stat but supports links)
			src = path.__str__() # if isinstance(path, Path) else path
			assert not os.path.islink(src), f"{self} {SOFTLINK_DISABLED_ERROR}"
			assert self.toSrcPath(src) == path, f"{self} input is not from remotefs"

			# take into account directories which have to be created
			cpath = self.toCachePath(path)
			in_between_dir_sizes = 0
			while not cpath.parent.exists():
				in_between_dir_sizes += self.MIN_DIR_SIZE
				cpath = cpath.parent

			cache_size = in_between_dir_sizes + os.path.getsize(src) + self.__current_CacheSize
			return cache_size <= self.__maxCacheSize

		assert False, f'{self}: Types are wrong: {path}({type(path)}) not in [Path, str, FileInfo]'

	def isFilledBy(self, percent: float) -> bool:
		""":param percent: between [0.0, 1.0]"""
		assert 0.0 <= percent <= 1.0, 'disk_isFullBy: needs to be [0-1]'
		diskUsage = self.__current_CacheSize / self.__maxCacheSize
		return True if diskUsage >= percent else False

	def isFull(self, use_threshold: bool = False) -> bool:
		if use_threshold:
			return self.isFilledBy(self.__cacheThreshold)
		return self.isFilledBy(1.0)

	# Book-Keeping related

	def track(self, path: str, reuse_ino=0) -> int:
		"""
		Add `path` to internal filing structure and reserve it's disk space
		reuse_ino: re-use an old inode
		"""

		# handling of create and mkdir (files dont exist yet so os.stat(path_) will throw an error)
		path_: str = self.toSrcPath(path).__str__()
		if not os.path.exists(path_):
			path_ = self.toCachePath(path).__str__()

		# get info about tracked file
		timestamp: int = getattr(os.stat(path_), self.time_attr) // self.__NANOSEC_PER_SEC__
		size: int = os.path.getsize(path_)
		src_path: str = self.toSrcPath(path).__str__()
		ino: int = self.path_to_ino(src_path, reuse_ino=reuse_ino)
		tracked_path = self.in_cache.get(timestamp)

		# save meta info
		assert isinstance(tracked_path, list) or isinstance(tracked_path, tuple) or tracked_path is None, "Type mismatch!"
		if isinstance(tracked_path, list):
			self.in_cache[timestamp].append((src_path, size))
		elif isinstance(tracked_path, tuple):
			self.in_cache[timestamp] = [self.in_cache[timestamp]] + [(src_path, size)]
		else:
			self.in_cache[timestamp] = (src_path, size)

		# update book-keeping
		self.path_timestamp[src_path] = timestamp
		self.__cached_inos[ino] = True
		self.__current_CacheSize += size
		return ino

	def untrack(self, path: str) -> None:
		"""Doesnt track `path` anymore and frees up its reserved size."""
		src_path: str = self.toSrcPath(path).__str__()
		if timestamp := self.path_timestamp.get(src_path):
			try:
				i: int = 0

				# assign references for a bit of a speedup / readablity
				in_cache = self.in_cache

				# getting the correct item by type
				og_item: Union[tuple[str, int], list[tuple[str, int]]] = in_cache[timestamp]
				if isinstance(og_item, list):
					i = [y[0] for y in og_item].index(src_path)
					item = og_item[i]
				else:
					item = og_item
				(t_path, size) = item[0], item[1]

				# actual clean up
				if isinstance(og_item, list):
					del og_item[i]
					if len(in_cache) == 0:
						del in_cache[timestamp]
				else:
					del in_cache[timestamp]

				del self.path_timestamp[src_path]
				del self.__cached_inos[self.path_to_ino(src_path)]
				self.__current_CacheSize -= size
			except KeyError as e:
				log.error("KeyError Exception that shouldnt have happened happened")
				log.exception(e)

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
		assert self.toSrcPath(path) == path, f"{path} doesn't have {self.sourceDir} prefix"
		self.__make_room_for_path(force, path, open_paths)

		if self.canStore(path):
			dest = self.toCachePath(path)
			addedDirsSize, addedFolders = self._cp_path(path, dest)
			self.__current_CacheSize += addedDirsSize
			# folders which are created by cp2Dir are untracked and should be tracked...
			# elements in cache doesnt model reality (too few entries too many undocumented)
			for parent in addedFolders:
				self.path_to_ino(parent)
				self.track(parent.__str__())

			if addedDirsSize == 0:
				self.path_to_ino(dest)
				self.track(path.__str__())

			# TODO: use xattributes later and make a custom field:
			# sth like __wolfs_atime__ : time.time_ns()
			#   for the last access of a file
			# so we retain standard-conformity

			return dest
		else:
			raise NotEnoughSpaceError('Not enough space')

	def __make_room_for_path(self, force: bool, path: Path, open_paths: list[Path] = None) -> None:
		def get_head_in_cache(self) -> tuple[str, int]:
			item: Union[tuple[str, int], list[tuple[str, int]]] = self.in_cache.peekitem(index=0)[1]
			if isinstance(item, list):
				item = item[0]
			src_path, size = item[0], item[1]
			return src_path, size

		if open_paths is None:
			open_paths = []
		while force and not self.canStore(path):
			try:
				(src_path, size) = get_head_in_cache(self)
				#assert timestamp in self.path_timestamp
				self.untrack(src_path)
			except IndexError:
				log.warning(f"Deleted all non open files and still couldn't store file: {path}")
				raise FUSEError(errno.EDQUOT)

			cpath: Path = Path(self.toCachePath(src_path))

			# skip open files (we cant sync and close them as they might be written / read from)
			if cpath in open_paths:
				continue

			assert cpath.exists(), f'File {Col(cpath)} not in cache although it should be ?'

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

	def _cp_path(self, src: Path_str, dst: Path_str) -> tuple[int, list[Path]]:
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
			addedDirsSize, addedFolders = self.mkdir_p(src)
		elif src.is_file():
			if not dst.parent.exists():
				addedDirsSize, addedFolders = self.mkdir_p(src.parent)
			shutil.copy2(src, dst)
		else:
			msg = f'{Col.BR} Unrecognized filetype: {src} -> ignoring'
			log.error(msg)
			raise IOError(msg)

		Disk.copystat(src, dst)
		return addedDirsSize, addedFolders

	@staticmethod
	def copystat(src: Path_str, dst: Path_str) -> None:
		"""Book-keeping of file-Attributes"""
		shutil.copystat(src, dst)

	# ===============
	# Print functions
	# ===============

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
		copySummary =\
			f'{Col.BW}Cache is currently storing {Col(self.getNumberOfElements())} elements and is {diskUsage} '\
			+ f' full (used: {usedCache} / {maxCache} )'
		return str(copySummary)

	def printSummary(self) -> None:
		print(self.getSummary())
