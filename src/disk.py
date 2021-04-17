#!/usr/bin/python

# suppress 'unused' warnings
from IPython import embed

embed = embed

from pathlib import Path
import shutil
import errno
import sys
import os
from util import Col
import logging

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

	def __init__(self, sourceDir: Path, cacheDir: Path, maxCacheSize: int, cacheThreshold=0.8):
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
		self.__diskUsagePercent = 1
		self.__maxCacheSize = maxCacheSize * self.__MEGABYTE__

	# ===========
	# private api
	# ===========

	def __updateCacheSize(self):
		"""only gets called internally for book-keeping"""
		self.__current_CacheSize = self.getSize(self.cacheDir)
		self.__diskUsagePercent = self.__current_CacheSize / self.__maxCacheSize
		# log.debug(f'new CacheSize:{self.__current_CacheSize}')
		# log.debug(f'{self.__diskUsagePercent} full')
		return

	# ==========
	# public api
	# ==========

	def toCachePath(self, path: Path) -> Path:
		return CachePath.toCachePath(self.sourceDir, self.cacheDir, path)

	def toSrcPath(self, path: Path) -> Path:
		return CachePath.toSrcPath(self.cacheDir, self.sourceDir, path)

	@staticmethod
	def cpdir(src: Path, dst: Path):
		"""creates a dir dst and all its missing parents. Copies attrs of src and its parents accordingly"""
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

	def canStore(self, path: Path):
		"""
		Cache size needs to be updated if file is inserted into cacheDir.
		:param path: returns False on symbolic links
		"""
		# TODO: maybe try to get filesize via lstat (like stat but supports links)
		p = path.__str__()
		if not os.path.islink(p) and os.path.getsize(p) + self.__current_CacheSize < self.__maxCacheSize:
			return True
		else:
			return False

	def isFilledBy(self, percent: float):
		"""percent: float needs to be between 0.0 and 1.0"""
		assert 0.0 < percent < 1.0, 'disk_isFullBy: needs to be [0-1]'
		diskUsage = self.getSize(self.cacheDir) / self.__maxCacheSize
		return True if diskUsage >= percent else False

	def isFull(self, use_threshold=False):
		if use_threshold:
			return self.isFilledBy(self.__cacheThreshold)
		return self.isFilledBy(1.0)

	def copyIntoCacheDir(self, src: Path) -> Path:
		"""
		Copy into Cache and keep meta-data.
		Creates parent directories on the fly.
		Ignores special files.

		:arg src File from sourceDir to be copied into cacheDir
		:returns: Path of copied file in cacheDir
		"""
		dst = self.toCachePath(src)
		src_mode = os.stat(src).st_mode

		if src.is_dir():
			self.cpdir(src, dst)
		elif src.is_file():
			if not dst.parent.exists():
				self.cpdir(src.parent, dst.parent)
			shutil.copy2(src, dst)
		else:
			msg = Col.by(f' Unrecognized filetype: {src} -> ignoring')
			log.error(msg)
			raise IOError(msg)

		# book-keeping of file-attributes (also takes care of parent dirs having wrong modes from previous runs)
		dst.chmod(src_mode)
		shutil.copystat(src, dst)
		self.__updateCacheSize()
		return dst

	def getCurrentStatus(self) -> tuple:
		""":returns: Current (fullness in %, usedCache, maxCache) of self.cacheDir"""
		diskUsage = self.__diskUsagePercent
		usedCache = self.__current_CacheSize
		maxCache = self.__maxCacheSize
		return diskUsage, usedCache, maxCache
