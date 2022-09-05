#!/usr/bin/env python
# The job of this module:
#  - translate Paths from backend, cache and the cache filesystem
#  - give an easy interface to how much disk space is used
#    -> needed to efficiently use and allocate space
#  - track files that are currently in cache
#    -> makes it easier to serve files that are already in the cache

# suppress 'unused' warnings

from IPython import embed
embed = embed

from pathlib import Path
import shutil
import errno
import os
import logging
from pyfuse3 import FUSEError
from src.libwolfs.util import Col, Path_str
from src.libwolfs.errors import NotEnoughSpaceError, SOFTLINK_DISABLED_ERROR
from typing import Union
from src.libwolfs.cache import Cache

log = logging.getLogger(__name__)

class Disk(Cache):

	# ==========
	# public api
	# ==========

	# Helpers
	def mkdir_p(self, src: Path, added_folders=None, added_size: int = 0) -> tuple[int, list[Path]]:
		"""
		Recursively re-creates the directory path up until `src` for `dst`.

		Bascially the same as "mkdir -P dst" with the caviat that the attributes of the corresponding `src`-directory
		  are copied too.

		returns: tuple of added empty folder sizes list of filepath to in between folders

		actually it is like generate in betweens
		"""
		dst: Path = self.trans.toTmp(src)

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
	def canStore(self, size_or_path: Path) -> bool:
		"""Does the cache have room  for `path` ?"""
		if isinstance(size_or_path, int):
			size: int = size_or_path
			return (size + self._current_CacheSize) < self.maxCacheSize

		elif isinstance(size_or_path, Path):
			path: Path = size_or_path
			# TODO: maybe try to get filesize via lstat (like stat but supports links)
			src = path.__str__() # if isinstance(path, Path) else path
			assert not os.path.islink(src), f"{self} {SOFTLINK_DISABLED_ERROR}"
			assert self.trans.toSrc(src) == path, f"{self} input is not from remotefs"

			# take into account directories which have to be created
			cpath = self.trans.toTmp(path)
			in_between_dir_sizes = 0
			while not cpath.parent.exists():
				in_between_dir_sizes += self.MIN_DIR_SIZE
				cpath = cpath.parent

			cache_size = in_between_dir_sizes + os.path.getsize(src) + self._current_CacheSize
			return cache_size <= self.maxCacheSize

		assert False, f'{self}: Types are wrong: {size_or_path}({type(size_or_path)}) not in [Path, str, FileInfo]'

	# Book-Keeping related

	def track(self, path: str, reuse_ino=0) -> int:
		"""
		Add `path` to internal filing structure and reserve its disk space
		reuse_ino: re-use an old inode
		"""

		# handling of create and mkdir (files don't exist yet so os.stat(path_) will throw an error)
		path_: str = self.trans.toSrc(path).__str__()
		if not os.path.exists(path_):
			path_ = self.trans.toTmp(path).__str__()

		# get info about tracked file
		timestamp: int = getattr(os.stat(path_), self.time_attr) // self.__NANOSEC_PER_SEC__
		size: int = os.path.getsize(path_)
		src_path: str = self.trans.toSrc(path).__str__()
		ino: int = self.trans.path_to_ino(src_path, reuse_ino=reuse_ino)
		tracked_path = self.in_cache.get(timestamp)

		# save meta info
		assert isinstance(tracked_path, list) or isinstance(tracked_path, tuple) or tracked_path is None, "Type mismatch!"
		if isinstance(tracked_path, list):
			self.in_cache[timestamp].append((src_path, size))
		elif isinstance(tracked_path, tuple):
			self.in_cache[timestamp] = [self.in_cache[timestamp]] + [(src_path, size)]
		else:
			self.in_cache[timestamp] = (src_path, size)

		# update bookkeeping
		self.path_timestamp[src_path] = timestamp
		self._cached_inos[ino] = True
		self._current_CacheSize += size
		return ino

	def untrack(self, path: str) -> None:
		"""Doesn't track `path` anymore and frees up its reserved size. Can be seen as a 'delete'"""
		src_path: str = self.trans.toSrc(path).__str__()
		timestamp = self.path_timestamp.get(src_path)
		if timestamp is None:
			return

		i: int = 0

		# assign references for a bit of a speedup / readablity
		in_cache = self.in_cache

		try:
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
			del self._cached_inos[self.trans.path_to_ino(src_path)]
			self._current_CacheSize -= size
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
		:raises FUSEError(errno.EDQUOT): if all non-open files were deleted and there still isn't enough room for `path`
		:returns: Cache path of copied file/dir
		Copy `file` and its meta-data into Cache. If `force` is set it deletes least recently used files until enough space is available.

		Note:
		  Make sure to sync the cache and remote before copying if using `force` as it could potentially delete
		  a dirty file which has not yet been sync'd and data would be lost
		  calls assert if this happens at the moment
		"""
		assert self.trans.toSrc(path) == path, f"{path} doesn't have {self.trans.sourceDir} prefix"
		self.__make_room_for_path(force, path, open_paths)

		if self.canStore(path):
			dest = self.trans.toTmp(path)
			addedDirsSize, addedFolders = self.__cp_path(path, dest)
			self._current_CacheSize += addedDirsSize
			# folders which are created by cp2Dir are untracked and should be tracked...
			# elements in cache doesn't model reality (too few entries too many undocumented)
			for parent in addedFolders:
				self.trans.path_to_ino(parent)
				self.track(parent.__str__())

			if addedDirsSize == 0:
				self.trans.path_to_ino(dest)
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

			cpath: Path = Path(self.trans.toTmp(src_path))

			# skip open files (we can't sync and close them as they might be written / read from)
			if cpath in open_paths:
				continue

			assert cpath.exists(), f'File {Col(cpath)} not in cache although it should be ?'

			if os.path.isfile(cpath):
				os.remove(cpath)
			elif os.path.isdir(cpath):
				try:
					os.rmdir(cpath)
				except OSError:
					# directory isn't empty though I don't know what to do at this point
					# as re-adding it isn't an option ( directory entries only change if files are added or deleted so)
					# furthermore it would break the heap
					# at least don't let the CacheSize get corrupted by this
					self._current_CacheSize += size

	def __cp_path(self, src: Path_str, dst: Path_str) -> tuple[int, list[Path]]:
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
			# just give 0 back as we effictively won't do anything and just skip it
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
		"""Bookkeeping of file-Attributes"""
		shutil.copystat(src, dst)

	# ===============
	# Print functions
	# ===============

