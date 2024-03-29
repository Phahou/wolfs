#!/usr/bin/env python
import dataclasses
from typing import Final
from pathlib import Path
import errno
import os
import logging
from sys import exit
log = logging.getLogger(__name__)
from src.libwolfs.util import Path_str

@dataclasses.dataclass
class MountFSDirectoryInfo:
	sourceDir: Path
	cacheDir: Path
	mountDir: Path

	def __init__(self, sourceDir: Path_str, cacheDir: Path_str, mountDir: Path_str):
		self.sourceDir = Path(sourceDir)
		self.cacheDir = Path(cacheDir)
		self.mountDir = Path(mountDir)

class CachePath(Path):
	@staticmethod
	def toRootPath(sourceDir: Path_str, cacheDir: Path_str, path: Path_str) -> str:
		"""Get the Path without the cache or src prefix"""
		_path: str = path if isinstance(path, str) else path.__str__()
		root = _path.replace(sourceDir.__str__(), '').replace(cacheDir.__str__(), '')
		return ('/' + root).replace('//', '/') if root else '/'

	@staticmethod
	def toDestPath(sourceDir: Path_str, destDir: Path_str, path: Path_str) -> Path:
		root = CachePath.toRootPath(sourceDir, destDir, path)
		result = f"{destDir.__str__()}{root}".replace('//', '/')
		return Path(result)


class DiskBase:
	ROOT_INODE: Final[int] = 1
	__MEGABYTE__: Final[int] = 1024 * 1024
	__NANOSEC_PER_SEC__: Final[int] = 1_000_000_000


class PathTranslator:
	sourceDir: Path
	cacheDir: Path
	mountDir: Path

	def __init__(self, mount_info: MountFSDirectoryInfo):
		def setattr_exit_on_failure(name: str, value: Path) -> None:
			# fixed issue that CachePath fails on getting the parent manually
			path = Path(os.path.abspath(value))
			if not path.exists():
				log.critical(f'[Errno {errno.ENOENT}] {os.strerror(errno.ENOENT)}: {path}')
				exit(errno.ENOENT)
			else:
				setattr(self, name, path)

		setattr_exit_on_failure('sourceDir', mount_info.sourceDir)
		setattr_exit_on_failure('cacheDir', mount_info.cacheDir)
		setattr_exit_on_failure('mountDir', mount_info.mountDir)

	def toRoot(self, path: Path_str) -> str:
		trimmed_src_cache_path = CachePath.toRootPath(self.sourceDir, self.cacheDir, path)
#		if trimmed_src_cache_path == '/':
#			return '/'
		# use mountDir twice as it won't change it: replace() replaces all occourences
		return CachePath.toRootPath(self.mountDir, self.mountDir, trimmed_src_cache_path)

	def toMnt(self, path: Path_str) -> Path:
		rpath = self.toRoot(path)
		result = CachePath.toDestPath(self.mountDir, self.mountDir, rpath)
		return result

	def toSrc(self, path: Path_str) -> Path:
		rpath = self.toRoot(path)
		result = CachePath.toDestPath(self.sourceDir, self.sourceDir, rpath)
		return result

	def toTmp(self, path: Path_str) -> Path:
		rpath = self.toRoot(path)
		result = CachePath.toDestPath(self.cacheDir, self.cacheDir, rpath)
		return result

	def getParent(self, path: Path_str) -> str:
		result: str = self.toRoot(path)
		if result.count('/') < 2:
			result = '/'
		else:
			result = result[:result.rfind('/')]
		return result


class InodeTranslator(PathTranslator, DiskBase):
	def __init__(self, mount_info: MountFSDirectoryInfo):
		super().__init__(mount_info)

		self.__last_ino: int = DiskBase.ROOT_INODE  # as the first ino is always 1 (ino 1 is for bad blocks but fuse doesn't act that way)
		self.__freed_inos: set[int] = set()
		self.__path_ino_map: dict[str, int] = dict()
		self.__ino_path_map: dict[int, str | {str}] = dict()

		# init root path by defining root ino as last used ino
		self.__path_ino_map["/"] = self.__last_ino
		self.__ino_path_map[self.__last_ino] = "/"

	def ino_exists(self, inode: int) -> bool:
		return self.__ino_path_map.get(inode, None) is not None

	def __delitem__(self, inode__path: tuple[int, str]) -> None:
		"""delete translation inode"""
		inode, path = inode__path
		assert inode == self.path_to_ino(path), "Logic Error: double deletion"

		rpath = self.toRoot(path)
		if isinstance(self.__ino_path_map[inode], str):
			assert rpath in self.ino_to_rpath(inode), "Consistency Error"

		maybe_path: str | {str} = self.__ino_path_map[inode]
		if isinstance(maybe_path, str):
			# normal path: path, ino (1:1)
			self.__freed_inos.add(inode)
			del self.__ino_path_map[inode]
		elif isinstance(maybe_path, set):

			# handling hardlinks: multiple paths -> ino (*:1)
			self.__ino_path_map[inode].remove(rpath)

			# return to original str type
			if len(self.__ino_path_map[inode]) == 1:
				self.__ino_path_map[inode] = self.__ino_path_map[inode].pop()
		del self.__path_ino_map[rpath]

	def path_to_ino(self, some_path: Path_str, reuse_ino=0) -> int:
		"""
		Maps paths to inodes. Creates them if necessary
		re-uses ino `reuse_ino` if != 0
		:raise ValueError instead of asserts on logic errors
		"""
		assert isinstance(some_path, Path_str), "Type Error"
		assert reuse_ino >= 0, "reuse_ino can't be negative"

		def __add_ino_path(ino: int, path: str) -> None:
			"""
			Add ino to internal mappings
			:param ino: inode corresponding to path
			:param path: path corresponding to inode
			:return:
			"""
			self.__path_ino_map[path] = ino

			if maybe_path := self.__ino_path_map.get(ino):
				if isinstance(maybe_path, str) and maybe_path != path:
					self.__ino_path_map[ino] = {maybe_path, path}
				elif isinstance(maybe_path, set):
					self.__ino_path_map[ino].add(path)
			else:
				self.__ino_path_map[ino] = path

		path: str = self.toRoot(some_path)

		if ino := self.__path_ino_map.get(path):
			ino = ino
		elif reuse_ino != 0:  # for rename operations
			if reuse_ino > self.__last_ino:
				# programming error
				raise ValueError(f"Reused ino is larger than largest generated ino {reuse_ino} > {self.__last_ino}")
			elif reuse_ino in self.__freed_inos:
				# normal operation
				ino = reuse_ino
			else:
				# invalid ino as it's already used
				raise ValueError(f"Reused ino {reuse_ino} is not in freed ino set {self.__freed_inos}")
		else:
			ino = self.__last_ino + 1
			self.__last_ino = ino

		__add_ino_path(ino, path)

		return ino

	def ino_to_rpath(self, ino: int, need_set: bool = False) -> str | set[str]:
		"""
		Reverse lookup function of self.path_to_ino
		:param need_set: if dealing with hardlinks e.g. renaming a file set this to True, as
			you have to decide for yourself which path shall be renamed of them
		:param ino: inode to be lookuped up
		:return:
			if ino has no hardlinks:
		    	corresponding root path of ino
			if ino has hardlinks and need_set == False:
				some root path corresponding to ino
			if ino has hardlinks and need_set == True:
			     set of root paths mapping ino
		"""
		path: str | set[str] = self.__ino_path_map.get(ino)
		if isinstance(path, set):
			if need_set:
				return path
			tmp_p = path.pop()
			path.add(tmp_p)
			path = tmp_p

		assert path is not None, "Logic Error"
		return path

	def add_hardlink(self, ino: int, hardlink_target: Path_str) -> None:
		"""
		add root path of `some_path` to ino entry resolving them to the same ino

		:param ino: inode with already existent path
		:param hardlink_target: path to add to ino entry
		"""
		def health_checks():
			# check before inserting in set as pop() might always return first(validated) inserted item
			assert Path(hardlink_target) in \
				{self.toTmp(hardlink_target), self.toMnt(hardlink_target), self.toSrc(hardlink_target)}, \
				"hardlink_target must have Mnt|Src|Tmp prefix!"

			assert not Path(hardlink_target).is_dir() \
				and not self.toTmp(hardlink_target).is_dir() \
				and not self.toSrc(hardlink_target).is_dir() \
				and not self.toMnt(hardlink_target).is_dir(), "hardlinks to directories are illegal"

			assert ino > 0, "inos cant be negative"
			assert ino not in self.__freed_inos, "ino can't reference a freed ino"
			assert ino in self.__ino_path_map, "ino has to have a path before creating a link!"
		health_checks()

		rpath = self.toRoot(hardlink_target)
		assert rpath not in self.__path_ino_map, "To be added rpath mustn't be already saved!"

		original_path = self.ino_to_rpath(ino)

		# check if original_path too if it shall be converted to a hardlink
		if isinstance(original_path, str):
			assert not self.toTmp(original_path).is_dir() \
				and not self.toSrc(original_path).is_dir() \
				and not self.toMnt(original_path).is_dir(), "hardlinks to directories are illegal"

		if isinstance(original_path, str):
			original_path = {original_path, rpath}
		else:
			original_path.add(rpath)
		self.__ino_path_map[ino] = original_path
		self.__path_ino_map[rpath] = ino

	def add_softlink(self, link_path: Path_str, target: Path_str) -> int:
		"""
		Add softlink `link_path` referencing `target` by
		creating a new ino &
		:param link_path: new softlink
		:param target: to be referenced path
		"""
		# TODO: figure out how symbolic links are referenced in pyfuse
		#       doesnt make sense to write sth here when I dont know how I shall
		#       reference it
		raise NotImplementedError()
		ino: int = self.path_to_ino(link_path)
		return ino
		# assert self.toRoot(link_path) != self.toRoot(target), "Softlink can't reference itself"

		# symlinks are allowed to reference themselves
		# symlink have a new inode

		# links can be a new inode, so we reuse code & adapt

################################################################################
# convinience functions (basically just shortcuts to commonly used operations)

	def ino_toTmp(self, ino: int) -> Path:
		return self.toTmp(self.ino_to_rpath(ino))
