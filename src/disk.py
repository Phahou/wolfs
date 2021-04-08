#!/usr/bin/python
from pathlib import Path
import shutil
import errno
import sys
import os
from util import Col

class Disk:
	__MEGABYTE__ = 1024 * 1024

	def __init__(self, logger, sourceDir: str, cacheDir: str, maxCacheSize: int, cacheThreshold=0.8):
		self.__logger = logger
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

	def isFull(self, use_threshold=False):
		if use_threshold:
			return self.isFilledBy(self.__cacheThreshold)
		return self.isFilledBy(1.0)

	def isFilledBy(self, percent: float):
		"""percent: float needs to be between 0.0 and 1.0"""
		assert 0.0 < percent < 1.0, 'disk_isFullBy: needs to be [0-1]'
		diskUsage = self.getSize(self.cacheDir) / self.__maxCacheSize
		return True if diskUsage >= percent else False

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

	def canStore(self, path: str):
		"""update Cache Size needs to be called if file is inserted into cacheDir"""
		if os.path.getsize(path) + self.__current_CacheSize < self.__maxCacheSize:
			return True
		else:
			return False

	def __updateCacheSize(self):
		"""only gets called internally for book-keeping"""
		self.__current_CacheSize = self.getSize(self.cacheDir)
		self.__diskUsagePercent = self.__current_CacheSize / self.__maxCacheSize
		# log.debug(f'new CacheSize:{self.__current_CacheSize}')
		# log.debug(f'{self.__diskUsagePercent} full')

	def copyIntoCacheDir(self, path: str) -> Path:
		"""
		Copy into Cache and keep meta-data.
		Creates parent directories on the fly.
		Ignores special files.

		:arg path File from sourceDir to be copied into cacheDir
		:returns: Path of copied file in cacheDir
		"""
		dest = path.replace(self.sourceDir.__str__(), self.cacheDir.__str__())
		src_p, dest_p = Path(path), Path(dest)

		if src_p.is_dir():
			if not dest_p.exists():
				dest_p.mkdir(parents=True)
		elif src_p.is_file():
			if not dest_p.parent.exists():
				dest_p.mkdir(parents=True)
			shutil.copy2(src_p, dest_p)
		else:
			self.__logger.error(f'{Col.BY} Unrecognized filetype: {src_p} -> ignoring')

		# book-keeping of file-attributes
		dest_p.chmod(src_p.stat().st_mode)
		shutil.copystat(src_p, dest_p)
		self.__updateCacheSize()
		return dest_p

	def getCurrentStatus(self) -> tuple:
		""":returns: Current (fullness in %, usedCache, maxCache) of self.cacheDir"""
		diskUsage = self.__diskUsagePercent
		usedCache = self.__current_CacheSize
		maxCache = self.__maxCacheSize
		return diskUsage, usedCache, maxCache
