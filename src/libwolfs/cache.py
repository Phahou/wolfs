#!/usr/bin/env python
from sortedcontainers import SortedDict
from typing import Final
from src.libwolfs.translator import DiskBase
from os import mkdir, rmdir, stat

class Cache(DiskBase):
	maxCacheSize: Final[int]
	MIN_DIR_SIZE: Final[int]

	def __init__(self, maxCacheSize: int, noatime: bool = True, cacheThreshold: float = 0.99):
		# get OS dependant minimum directory size
		mkdir('wolfs_tmp_directory')
		self.MIN_DIR_SIZE = stat('wolfs_tmp_directory').st_size
		rmdir('wolfs_tmp_directory')

		self._current_CacheSize: int = 0
		self._cacheThreshold: float = cacheThreshold
		self.maxCacheSize = maxCacheSize * self.__MEGABYTE__

		self.time_attr: str = 'st_mtime_ns' if noatime else 'st_atime_ns'  # remote has mountopt noatime set?

		self.in_cache: SortedDict[int, (str, int)] = SortedDict()
		self.path_timestamp: dict[str, int] = dict()
		self._cached_inos: dict[int, bool] = dict()

	# fullness of cache
	def __le__(self, other: int) -> bool:
		"""Is the cache large enough to hold `size` ?"""
		return other <= self.maxCacheSize

	def __lt__(self, other: int) -> bool:
		return other < self.maxCacheSize

	def __gt__(self, other: int) -> bool:
		return other > self.maxCacheSize

	def __ge__(self, other: int) -> bool:
		return other >= self.maxCacheSize
