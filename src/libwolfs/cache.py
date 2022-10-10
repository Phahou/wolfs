#!/usr/bin/env python
from sortedcontainers import SortedDict
from typing import Final
from pathlib import Path
from src.libwolfs.translator import InodeTranslator, MountFSDirectoryInfo
from os import mkdir, rmdir, stat
from src.libwolfs.util import Col, Path_str, formatByteSize

class Cache(InodeTranslator):
	maxCacheSize: Final[int]
	MIN_DIR_SIZE: Final[int]

	def __init__(self, mount_info: MountFSDirectoryInfo,
			maxCacheSize: int, noatime: bool = True, cacheThreshold: float = 0.99):
		super().__init__(mount_info)

		def get_min_dir_size():
			tmpdir: Path = self.cacheDir / 'wolfs_tmp_directory'
			mkdir(tmpdir)
			min_dir_size = stat(tmpdir).st_size
			rmdir(tmpdir)
			return min_dir_size

		# get OS dependant minimum directory size
		self.MIN_DIR_SIZE = get_min_dir_size()

		self._current_CacheSize: int = 0
		self._cacheThreshold: float = cacheThreshold
		self.maxCacheSize = maxCacheSize * self.__MEGABYTE__

		self.time_attr: str = 'st_mtime_ns' if noatime else 'st_atime_ns'  # remote has mountopt noatime set?

		self.in_cache: SortedDict[int, (str, int)] = SortedDict()
		self.path_timestamp: dict[str, int] = dict()
		self._cached_inos: set[int] = set()

		# compatibility to old code use remove later
		self.trans = self

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

	def isFull(self, use_threshold: bool = False) -> bool:
		def isFilledBy(percent: float) -> bool:
			""":param percent: between [0.0, 1.0]"""
			assert 0.0 <= percent <= 1.0, 'disk_isFullBy: needs to be [0-1]'
			diskUsage = self._current_CacheSize / self.maxCacheSize
			return True if diskUsage >= percent else False

		percentage = self._cacheThreshold if use_threshold else 1.0
		return isFilledBy(percentage)

	def getSummary(self) -> str:
		diskUsage = Col.path(f'{Col.BY}{(100 * self._current_CacheSize / self.maxCacheSize):.8f}%')
		usedCache = Col.path(formatByteSize(self._current_CacheSize))
		MAX_CACHE_SIZE = Col.path(formatByteSize(self.maxCacheSize))
		copySummary =\
			f'{Col.BW}Cache is currently storing {Col(len(self.in_cache))} elements and is {diskUsage} '\
			+ f' full (used: {usedCache} / {MAX_CACHE_SIZE} )'
		return str(copySummary)