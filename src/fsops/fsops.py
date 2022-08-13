#!/usr/bin/env python3
# The job of this module:
#  - save & load previously mounted filesystem, so we can mount w/o reindexing
#  - initizalition of filesystem (generating directory and file data structures
#  - fill the cache with most recently used files

# suppress 'unused' warnings
import pyfuse3
from IPython import embed

embed = embed

import os
from src.remote import RemoteNode # type: ignore
import faulthandler

faulthandler.enable()

from pathlib import Path
from logging import getLogger

log = getLogger(__name__)

from src.libwolfs.util import Col, MaxPrioQueue
from src.fsops.vfsops import VFSOps
from src.libwolfs.fileInfo import FileInfo, DirInfo
from src.libwolfs.errors import NotEnoughSpaceError
import pickle
from typing import Any, Final, cast
from src.fsops.dirent import DirentOps

def save_obj(obj: Any, name: Path) -> None:
	with open(name, 'wb+') as f:
		pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(path: Path) -> Any:
	with open(path, 'rb') as f:
		return pickle.load(f)


class Wolfs(DirentOps):
	enable_writeback_cache: Final[bool] = True
	enable_acl: Final[bool] = True
	__metadb: Path

	def __init__(self, node: RemoteNode,
				 sourceDir: str, cacheDir: str, metadb: str = '', logFile: Path = Path(VFSOps._STDOUT),
				 noatime: bool = True, maxCacheSizeMB: int = VFSOps._DEFAULT_CACHE_SIZE):
		super().__init__(node, Path(sourceDir), Path(cacheDir), Path(logFile), maxCacheSizeMB, noatime)
		self.__metadb = Path(metadb)
		# todo / idea:
		#  - we could use the XDG / freedesktop spec for a the file location of the meta file (~/.config/wolfs/metaFile.db)
		#  - maybe use same location for config options later on idk (~/.config/wolfs/config.ini)
		#  - probably use same spec for the .cache data Directory if none was selected (~/.cache/wolfs/)
		# unused for now:
		# if 'archiv' in metadb:
		# if self.__ISMOUNTED:
		# transfer_q = self.populate_inode_maps(self.disk.sourceDir)
		# self.copyRecentFilesIntoCache(transfer_q)
		# else:
		# remote is offline our best guess is to believe that the cache is up to date
		# todo: set to poll is remote is mounted and replace metafile if so
		# self.load_internal_state(self.__metadb)
		# try:
		#	#self.vfs.inode_path_map = load_obj(metadb)
		# except FileNotFoundError or EOFError:
		#	print(f'File not found {metadb}')
		# except EOFError:
		#	# file was corrupted in last run
		transfer_q = self.populate_inode_maps(self.disk.sourceDir)
		self.copyRecentFilesIntoCache(transfer_q)

	def populate_inode_maps(self, root: Path) -> MaxPrioQueue:
		"""
		index the sourceDir filesystem tree
		:param root: root directory to add to filesystem to
		:param self.time_attr decides if mtime or atime is used
		:return: MaxPrioQueue() with most recently edited / accessed files (atime / mtime)
		"""
		assert isinstance(root, Path), f"{self}root({root}) must be of type str"

		def print_progress(indexedFileNr: int, func_str: str, st_ino: int, path: str) -> int:
			if indexedFileNr in range(0, 1_000_000, 1_000):
				log.debug(f'{Col.BY}(#{i}) {Col.BC}{func_str}: {Col.BY}{Col.inode(st_ino)} -> {path}')
			return i + 1

		transfer_q = MaxPrioQueue()

		def push_to_queue(ino: int, dir_attrs: pyfuse3.EntryAttributes) -> None:
			last_used = getattr(dir_attrs, self.disk.time_attr) // self.disk.__NANOSEC_PER_SEC__
			transfer_q.push_nowait((last_used, (ino, dir_attrs.st_size)))

		i: int = 0
		islink = os.path.islink

		def add_subdirectories(dirpath: str, dirnames: [str], filenames: [str]) -> (int, [str]):
			dir_attrs = FileInfo.getattr(path=dirpath)
			dir_inode = self.disk.trans.path_to_ino(dirpath)
			dir_attrs.st_ino = dir_inode

			# calculate inode_p by deriving the parent path
			# root_path: str = self.disk.trans.toRoot(dirpath)
			# inode_p_path: str = '/'
			# if j := root_path.rfind('/'):  # != 0
			# 	inode_p_path = root_path[:j]
			# inode_p: int = self.disk.trans.path_to_ino(inode_p_path)

			# filter out softlinks and prepare to absolute paths
			abs_dirnames = list(filter(lambda x: not islink(x), map(lambda p: os.path.join(dirpath, p), dirnames)))
			abs_filenames = list(filter(lambda x: not islink(x), map(lambda p: os.path.join(dirpath, p), filenames)))
			subdir_inos = list(map(lambda p: self.disk.trans.path_to_ino(p), abs_dirnames))

			# TODO:
			#   uhhh ich denk wir haben immernoch das problem das die einträge doppelt sind bei Ordnern bei Dateien ist es nicht so
			#   gut is das wir n indirekten zugang zu der add_Directory haben könnte man eigentlich auch für Filepath und add_path verwenden
			#   um die eigentliche funktion kurz zu halten aber die konstistenz checks vollständig zu haben
			self.add_Directory(dirpath)
			self.add_subDirectories(subdir_inos, inode_p=dir_inode)
			push_to_queue(dir_inode, dir_attrs)

			return dir_inode, abs_filenames

		# reminder that drives actually have very small inode numbers (e.g. 2 or 5)
		for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
			if islink(dirpath):
				continue
			dir_inode, filenames = add_subdirectories(dirpath, dirnames, filenames)

			for f in filenames:
				file_attrs = FileInfo.getattr(path=f)
				st_ino = self.disk.trans.path_to_ino(f)
				file_attrs.st_ino = st_ino
				push_to_queue(st_ino, file_attrs)

				self.vfs.addFilePath(dir_inode, st_ino, f, file_attrs)
				i = print_progress(i, 'add_path', st_ino, f)
			i = print_progress(i, 'add_Directory', dir_inode, dirpath)

		# remove doubly added root inode, do it here as the internal add function would need another if then
		self.vfs.inode_path_map[self.disk.ROOT_INODE].children.remove(self.disk.ROOT_INODE)

		for k, v in self.vfs.inode_path_map.items():
			assert k == v.entry.st_ino
			if isinstance(v, DirInfo):
				self.vfs.inode_path_map[k] = DirInfo(cast(Path, v.src), cast(Path, v.cache), v.entry, sorted(v.children))

		return transfer_q

	def copyRecentFilesIntoCache(self, transfer_q: MaxPrioQueue) -> None:
		print(f'{Col.B}Transfering files...{Col.END}')
		while not transfer_q.empty() and not self.disk.isFull(use_threshold=True):
			timestamp, (inode, file_size) = transfer_q.pop_nowait()
			info: FileInfo = self.vfs.inode_path_map[inode]
			path, dst = cast(Path, info.src), info.cache

			# skip symbolic links for now
			if os.path.islink(path):
				continue

			try:
				self.disk.cp2Cache(path)
			except NotEnoughSpaceError:
				# filter the Queue
				purged_list = MaxPrioQueue()
				while not transfer_q.empty():
					timestamp_i, (inode_i, size_i) = transfer_q.pop_nowait()
					if size_i < file_size:
						purged_list.push_nowait((timestamp_i, (inode_i, size_i)))
				transfer_q = purged_list

		print(f'{Col.BW}Finished transfering. {self.disk.getSummary()}')

	def save_internal_state(self) -> None:
		save_obj(self.vfs.inode_path_map, self.__metadb)

	def load_internal_state(self, metadb: Path) -> None:
		# load inodes_path_map from meta-data file
		# if metadb.exists() and metadb.is_file():
		# ...
		try:
			self.vfs.inode_path_map = load_obj(metadb)
			return
		except FileNotFoundError or EOFError:
			print(f'File not found {metadb} defaulting to ' + '{}')
			self.vfs.inode_path_map = {}
