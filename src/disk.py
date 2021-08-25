#!/usr/bin/python

# suppress 'unused' warnings
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
import typing
from sortedcontainers import SortedDict


log = logging.getLogger(__name__)


class CachePath(Path):
	@staticmethod
	def toSrcPath(sourceDir, cacheDir, path) -> Path:
		return Path(path.__str__().replace(cacheDir.__str__(), sourceDir.__str__()))

	@staticmethod
	def toCachePath(sourceDir: Path, cacheDir: Path, path: Path) -> Path:
		return Path(path.__str__().replace(sourceDir.__str__(), cacheDir.__str__()))


class Disk:
	Path_str = typing.TypeVar('Path_str', Path, str)
	__MEGABYTE__ = 1024 * 1024
	__NANOSEC_PER_SEC__ = 1_000_000_000
	ROOT_INODE = 2

	def __init__(self, sourceDir: Path, cacheDir: Path, maxCacheSize: int, noatime: bool, cacheThreshold=0.99):
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
		self.__current_CacheSize = 0
		self.__cacheThreshold = cacheThreshold
		self.__maxCacheSize = maxCacheSize * self.__MEGABYTE__
		self.time_attr = 'st_mtime_ns' if noatime else 'st_atime_ns'  # remote has mountopt noatime set?

		self.in_cache: SortedDict = SortedDict()
		self.path_timestamp = dict()
		self.__ino_cache: dict[int: str] = dict()

		# for mnt_ino_translation and path_to_ino mapping functions
		self.__mnt_ino2st_ino: dict[int, int] = {pyfuse3.ROOT_INODE: self.ROOT_INODE, self.ROOT_INODE: self.ROOT_INODE}
		self.__tmp_ino2st_ino: dict[int, int] = {pyfuse3.ROOT_INODE: self.ROOT_INODE, self.ROOT_INODE: self.ROOT_INODE}
		self.__last_ino = 1			# as the first ino is always 2 (ino 1 is for bad blocks but fuse doesnt act that way)
		self.__freed_inos = []
		self.path_ino_map: dict[int: int] = dict()

	def getNumberOfElements(self):
		return len(self.in_cache)

	# ===========
	# private api
	# ===========

	def isInCache(self, ino: int):
		return self.__ino_cache.get(ino, False)

	def _rebuildCacheDir(self):
		"""
		Rebuild the internal book-keeping ( `self.__curent_CacheSize` , `self.in_cache` ) variables\n
		based on the contents currently in `self.cacheDir`
		"""
		self.in_cache = []
		self.in_cache: SortedDict = SortedDict()
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

	def toCachePath(self, path: Path_str) -> Path_str:
		return CachePath.toCachePath(self.sourceDir, self.cacheDir, path)

	def toSrcPath(self, path: Path_str) -> Path_str:
		return CachePath.toSrcPath(self.sourceDir, self.cacheDir, path)

	def toRootPath(self, path: Path_str) -> Path_str:
		"""Get the Path without the cache or src prefix"""
		path = self.toSrcPath(path).__str__().replace(self.sourceDir.__str__(), '')
		return path if path else '/'

	def mnt_ino_translation(self, inode: int):
		return inode if inode != pyfuse3.ROOT_INODE else self.ROOT_INODE

	def path_to_ino(self, some_path) -> int:
		"""Maps paths to inodes. Creates them if necessary"""
		path = self.toRootPath(some_path).__str__()  # we dont really care where the file is exactly

		if ino := self.path_ino_map.get(path):
			ino = ino
		elif self.__freed_inos:
			ino = self.__freed_inos[0]
			del self.__freed_inos[0]
		else:
			ino = self.__last_ino + 1
			self.__last_ino += 1
		self.path_ino_map[path] = ino

		if os.path.exists(some_path):  # update foreign translation only if accessible
			foreign_ino = os.stat(some_path).st_ino
			if some_path.__str__().startswith(self.sourceDir.__str__()):
				self.__mnt_ino2st_ino[foreign_ino] = ino
			else:
				self.__tmp_ino2st_ino[foreign_ino] = ino
		return ino

	@staticmethod
	def cpdir(src: Path, dst: Path, added_size=0):
		"""creates a dir  `dst` and all its missing parents. Copies attrs of `src` and its parents accordingly"""
		if not dst.exists():
			if not dst.parent.exists():
				added_size += Disk.cpdir(src.parent, dst.parent)
			stat_result = os.stat(src)
			mode = stat_result.st_mode
			added_size += stat_result.st_size
			dst.mkdir(mode=mode, parents=False)
		shutil.copystat(src, dst)
		return added_size

	@staticmethod
	def getSize(path='.'):
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

	def canStore(self, path) -> bool:
		return self.__canStore(path)

	def __canStore(self, path) -> bool:
		"""
		Does the cache have room  for `path` ?
		Cache size needs to be updated if file is inserted into cacheDir.
		:param path: returns False on symbolic links
		"""
		if isinstance(path, FileInfo):
			store_able = not os.path.islink(
				path.cache) and path.entry.st_size + self.__current_CacheSize < self.__maxCacheSize
		elif isinstance(path, Path) or isinstance(path, str):
			# TODO: maybe try to get filesize via lstat (like stat but supports links)
			p = path.__str__() if isinstance(path, Path) else path
			store_able = not os.path.islink(p) and os.path.getsize(p) + self.__current_CacheSize < self.__maxCacheSize
		else:
			store_able = ""
			assert f'{__functionName__(self)}: Types are wrong: {path}({type(path)}) not in [Path, str, FileInfo]'
		return store_able

	def isFilledBy(self, percent: float):
		""":param percent: between [0.0, 1.0]"""
		assert 0.0 < percent < 1.0, 'disk_isFullBy: needs to be [0-1]'
		diskUsage = self.__current_CacheSize / self.__maxCacheSize
		return True if diskUsage >= percent else False

	def isFull(self, use_threshold=False):
		if use_threshold:
			return self.isFilledBy(self.__cacheThreshold)
		return self.isFilledBy(1.0)

	def untrack(self, path: str):
		"""Doesnt track `path` anymore and frees up its reserved size"""
		path = self.toSrcPath(path)
		if timestamp := self.path_timestamp.get(path):
			(t_path, size) = self.in_cache[timestamp]
			self.__current_CacheSize -= size
			del self.in_cache[timestamp]
			del self.path_timestamp[path]
			del self.__ino_cache[self.path_to_ino(path)]

	def track(self, path, force=False):
		assert self.sourceDir.__str__() in path.__str__(),\
			f'Path: {path} should only have prefixes of {self.sourceDir.__str__()}'

		timestamp = time.time_ns() if force else getattr(os.stat(path), self.time_attr) // self.__NANOSEC_PER_SEC__
		size = os.path.getsize(path)
		self.in_cache[timestamp] = (path, size)
		self.path_timestamp[path] = timestamp
		self.__ino_cache[self.path_to_ino(path)] = True
		self.__current_CacheSize +=size

	# todo: think about making a write cache for newly created files -> store write_ops
	#       check after a timeout if said files still exist or are still referenced if not
	#       then they were tempfiles anyway otherwise sync them to the backend
	def cp2Cache(self, path: Path_str, force=False, open_paths=[]) -> Path:
		"""
		:param force: Delete files if necessary
		:param path: file/dir to be copied
		:raises NotEnoughSpaceError: If there isn't enough space to hold file and `force` wasn't set
		:returns: Cache path of copied file/dir
		Copy `file` and its meta-data into Cache. If `force` is set it deletes least recently used files until enough space is available.

		Note:
		  Make sure to sync the cache and remote before copying if using `force` as it could potentially delete
		  a dirty file which has not yet been sync'd and data would be lost
		"""
		if open_paths is None:
			open_paths = []
		while force and not self.__canStore(path):
			try:
				timestamp, (src_path, size) = self.in_cache.get(index=0)
				assert timestamp in self.path_timestamp
				self.untrack(src_path)
			except KeyError:
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

		if self.__canStore(path):
			dest = self.toCachePath(path)
			addedDirsSize = Disk._cp2Dir(path, dest)
			self.path_to_ino(dest)
			self.track(path, force)
			self.__current_CacheSize += addedDirsSize

			# TODO: use xattributes later and make a custom field:
			# sth like __wolfs_atime__ : time.time_ns()
			#   for the last access of a file
			# so we retain standard-conformity

			return dest
		else:
			raise NotEnoughSpaceError('Not enough space')

	@staticmethod
	def _cp2Dir(src: Path_str, dst: Path_str):
		"""
		Create a copy of `src` in `dst` while also keeping meta-data.
		Creates parent directories on the fly.
		Ignores special files.

		:arg src: original File
		:arg dst: destination File to have the same attributes and contents of `src` after the call
		:returns: Path of copied file in cacheDir
		"""
		src, dst = Path(src), Path(dst)
		addedDirsSize = 0
		if src == dst:
			# same File no need for copying,
			# file should exist already too as self.__canStore would throw errors otherwise
			return src

		if src.is_dir():
			addedDirsSize = Disk.cpdir(src, dst)
		elif src.is_file():
			if not dst.parent.exists():
				addedDirsSize = Disk.cpdir(src.parent, dst.parent)
			shutil.copy2(src, dst)
		else:
			msg = f'{Col.BY} Unrecognized filetype: {src} -> ignoring'
			log.error(msg)
			raise IOError(msg)

		Disk.cpAttrs(src, dst)
		return addedDirsSize

	@staticmethod
	def cpAttrs(src: Path_str, dst: Path_str):
		# book-keeping of file-attributes (also takes care of parent dirs having wrong modes from previous runs)
		src_mode = os.stat(src).st_mode
		dst.chmod(src_mode)
		shutil.copystat(src, dst)

	def getCurrentStatus(self) -> tuple:
		""":returns: Current (fullness in %, usedCache, maxCache) of self.cacheDir"""
		usedCache = self.__current_CacheSize
		maxCache = self.__maxCacheSize
		return 100 * usedCache / maxCache, usedCache, maxCache

	def getSummary(self):
		diskUsage, usedCache, maxCache = self.getCurrentStatus()
		diskUsage = Col.path(f'{Col.BY}{diskUsage:.8f}%')
		usedCache = Col.path(formatByteSize(usedCache))
		maxCache = Col.path(formatByteSize(maxCache))
		copySummary = Col.BW \
			+ f'Cache is currently storing {Col.inode(self.getNumberOfElements())} elements and is {diskUsage} '\
			+ f' full (used: {usedCache} / {maxCache} )'
		return copySummary

	def printSummary(self):
		print(self.getSummary())
