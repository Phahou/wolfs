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
from errors import NotEnoughSpaceError
import heapq
from queue import PriorityQueue

log = logging.getLogger(__name__)


class CachePath(Path):
	@staticmethod
	def toSrcPath(sourceDir, cacheDir, path) -> Path:
		return Path(path.__str__().replace(cacheDir.__str__(), sourceDir.__str__()))

	@staticmethod
	def toCachePath(sourceDir: Path, cacheDir: Path, path: Path) -> Path:
		return Path(path.__str__().replace(sourceDir.__str__(), cacheDir.__str__()))


class Disk:
	__MEGABYTE__ = 1024 * 1024
	__NANOSEC_PER_SEC__ = 1_000_000_000

	def __init__(self, sourceDir: Path, cacheDir: Path, maxCacheSize: int, noatime: bool, cacheThreshold=0.99):
		# fs related:
		self.sourceDir = Path(sourceDir)
		if not self.sourceDir.exists():
			print(f'[{errno.ENOENT}] No such file or directory: {sourceDir}')
			sys.exit(errno.ENOENT)

		self.cacheDir = Path(cacheDir)
		if not self.cacheDir.exists():
			print(f'[{errno.ENOENT}] No such file or directory: {cacheDir}')
			sys.exit(errno.ENOENT)

		# cache related:
		self.__current_CacheSize = 0
		self.__cacheThreshold = cacheThreshold
		self.__maxCacheSize = maxCacheSize * self.__MEGABYTE__
		self.time_attr = 'st_mtime_ns' if noatime else 'st_atime_ns'  # remote has mountopt noatime set?
		self.in_cache = PriorityQueue()
		self.in_cache2 = []

	# ===========
	# private api
	# ===========

	# ==========
	# public api
	# ==========

	def toCachePath(self, path: Path) -> Path:
		return CachePath.toCachePath(self.sourceDir, self.cacheDir, path)

	def toSrcPath(self, path: Path) -> Path:
		return CachePath.toSrcPath(self.sourceDir, self.cacheDir, path)

	@staticmethod
	def cpdir(src: Path, dst: Path):
		"""creates a dir  `dst` and all its missing parents. Copies attrs of `src` and its parents accordingly"""
		if not dst.exists():
			if not dst.parent.exists():
				Disk.cpdir(src.parent, dst.parent)
			mode = os.stat(src).st_mode
			dst.mkdir(mode=mode, parents=False)
		shutil.copystat(src, dst)

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
		else:
			# TODO: maybe try to get filesize via lstat (like stat but supports links)
			p = path.__str__()
			store_able = not os.path.islink(p) and os.path.getsize(p) + self.__current_CacheSize < self.__maxCacheSize
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

	def _pushtoHeap(self, path, force=False):
		timestamp = time.time_ns() if force else getattr(os.stat(path), self.time_attr) // self.__NANOSEC_PER_SEC__
		size = os.path.getsize(path)
		# st_ino = os.stat(path).st_ino
		heapq.heappush(self.in_cache2, (timestamp, (path, size)))
		self.in_cache.put_nowait((timestamp, (path, size)))
		return size

	def _deleteInHeap(self, path):
		pass

	def addFile(self, path: str, attr: pyfuse3.EntryAttributes):
		# todo: think about making a write cache for newly created files -> store write_ops
		#       check after a timeout if said files still exist or are still referenced if not
		#       then they were tempfiles anyway otherwise sync them to the backend
		pass

	def cp2Cache(self, path: Path, force=False, open_paths=[]) -> Path:
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
		while force and not self.__canStore(path):
			try:
				c_timestamp, (c_src, c_size) = self.in_cache.get_nowait()
			except queue.Empty:
				log.warning(f"Deleted all non open files and still couldn't store file: {path}")
				raise FUSEError(errno.EDQUOT)

			c_timestamp2, (c_src2, c_size2) = heapq.heappop(self.in_cache2)
			cpath: Path = Path(self.toCachePath(c_src))

			# skip open files (we cant sync and close them as they might be written / read from)
			if cpath in open_paths:
				continue

			if not cpath.exists():
				raise FUSEError('File not in cache although it should be ?')

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
					self.__current_CacheSize += c_size

			self.__current_CacheSize -= c_size

		if self.__canStore(path):
			dest = self.toCachePath(path)
			Disk._cp2Dir(path, dest)
			self.__current_CacheSize += self._pushtoHeap(path, force)

			# TODO: use xattributes later and make a custom field:
			# sth like __wolfs_atime__ : time.time_ns()
			#   for the last access of a file
			# so we retain standard-conformity

			return dest
		else:
			raise NotEnoughSpaceError('Not enough space')

	@staticmethod
	def _cp2Dir(src: Path, dst: Path):
		"""
		Create a copy of `src` in `dst` while also keeping meta-data.
		Creates parent directories on the fly.
		Ignores special files.

		:arg src: original File
		:arg dst: destination File to have the same attributes and contents of `src` after the call
		:returns: Path of copied file in cacheDir
		"""
		if src == dst:
			# same File no need for copying,
			# file should exist already too as self.__canStore would throw errors otherwise
			return src
		src_mode = os.stat(src).st_mode

		if src.is_dir():
			Disk.cpdir(src, dst)
		elif src.is_file():
			if not dst.parent.exists():
				Disk.cpdir(src.parent, dst.parent)
			shutil.copy2(src, dst)
		else:
			msg = Col.by(f' Unrecognized filetype: {src} -> ignoring')
			log.error(msg)
			raise IOError(msg)

		# book-keeping of file-attributes (also takes care of parent dirs having wrong modes from previous runs)
		dst.chmod(src_mode)
		shutil.copystat(src, dst)
		return dst

	def getCurrentStatus(self) -> tuple:
		""":returns: Current (fullness in %, usedCache, maxCache) of self.cacheDir"""
		usedCache = self.__current_CacheSize
		maxCache = self.__maxCacheSize
		return 100 * usedCache / maxCache, usedCache, maxCache

	def printSummary(self):
		diskUsage, usedCache, maxCache = self.getCurrentStatus()
		diskUsage = Col.by(f'{diskUsage:.8f}%')
		usedCache = Col.by(f'{Col.BY}{formatByteSize(usedCache)} ')
		maxCache = Col.by(f'{formatByteSize(maxCache)} ')
		copySummary = \
			Col.bw(
				f'Cache is currently storing {self.in_cache.qsize()} elements and is {diskUsage} ') + Col.bw(
				'full\n') + \
			Col.bw(f" (used: {usedCache}") + Col.bw(f" / {maxCache}") + Col.bw(")")
		print(copySummary)
