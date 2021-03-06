#!/usr/bin/env python3

# suppress 'unused' warnings
import pyfuse3
from IPython import embed

embed = embed

import os
from remote import RemoteNode # type: ignore
import faulthandler

faulthandler.enable()

from pathlib import Path
from logging import getLogger

log = getLogger(__name__)

from util import formatByteSize, Col, MaxPrioQueue
from vfsops import VFSOps
from fileInfo import FileInfo, DirInfo
from errors import NotEnoughSpaceError
import pickle
from typing import Any, Final, cast
from dirent import DirentOps

def save_obj(obj: Any, name: Path) -> None:
	with open(name, 'wb+') as f:
		pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(path: Path) -> Any:
	with open(path, 'rb') as f:
		return pickle.load(f)


class Wolfs(DirentOps):
	enable_writeback_cache: Final[bool] = True
	enable_acl: Final[bool] = True

	def __init__(self, node: RemoteNode,
				 sourceDir: str, cacheDir: str, metadb: str = '', logFile: Path = Path(VFSOps._STDOUT),
				 noatime: bool = True, maxCacheSizeMB: int = VFSOps._DEFAULT_CACHE_SIZE):
		super().__init__(node, Path(sourceDir), Path(cacheDir), Path(logFile), maxCacheSizeMB, noatime)
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
		# self.restoreInternalState(Path(metadb))
		# try:
		#	#self.vfs.inode_path_map = load_obj(metadb)
		# except FileNotFoundError or EOFError:
		#	print(f'File not found {metadb}')
		# except EOFError:
		#	# file was corrupted in last run
		transfer_q = self.populate_inode_maps(self.disk.sourceDir)
		self.copyRecentFilesIntoCache(transfer_q)
		save_obj(self.vfs.inode_path_map, Path(metadb))

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

		i = 0
		islink = os.path.islink
		# reminder that drives actually have very small inode numbers (e.g. 2 or 5)
		for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
			if islink(dirpath):
				continue
			dir_attrs = FileInfo.getattr(path=dirpath)
			inode_p = self.path_to_ino(dirpath)
			dir_attrs.st_ino = inode_p
			# print(inode_p, end=', ')
			directory: DirInfo = self.vfs.add_Directory(inode_p, dirpath, dir_attrs, [])

			push_to_queue(inode_p, dir_attrs)

			for d in dirnames:
				# only add child inodes here the subdirs will be walked through
				subdir_path = os.path.join(dirpath, d)
				if islink(subdir_path):
					continue
				directory.children.append(self.path_to_ino(subdir_path))

			# filepaths, fileattrs = [], []
			for f in filenames:
				filepath = os.path.join(dirpath, f)
				if islink(filepath):
					continue
				file_attrs = FileInfo.getattr(path=filepath)
				st_ino = self.path_to_ino(filepath)
				file_attrs.st_ino = st_ino
				push_to_queue(st_ino, file_attrs)
				assert self.path_to_ino(filepath) == self.path_to_ino(filepath), 'path != same path'
				self.vfs.addFilePath(inode_p, st_ino, filepath, file_attrs)
				i = print_progress(i, 'add_path', st_ino, filepath)
			i = print_progress(i, 'add_Directory', inode_p, dirpath)

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

		# print summary
		diskUsage, usedCache, maxCache = self.disk.getCurrentStatus()
		diskUsage = Col.path(f'{diskUsage:.8f}%')
		usedCache = Col.path(f'{Col.BY}{formatByteSize(usedCache)}')
		maxCache = Col.path(f'{formatByteSize(maxCache)}')
		copySummary = \
			f'Finished transfering {self.disk.getNumberOfElements()} elements.\nCache is now {diskUsage} full' + \
			f' (used: {usedCache} / {maxCache})'
		print(copySummary)

	def restoreInternalState(self, metadb: Path) -> None:
		# load inodes_path_map from meta-data file
		# if metadb.exists() and metadb.is_file():
		# ...
		try:
			self.vfs.inode_path_map = load_obj(metadb)
			return
		except FileNotFoundError or EOFError:
			print(f'File not found {metadb}')
		# except EOFError:
		# file was corrupted in last run
		transfer_q = self.populate_inode_maps(self.disk.sourceDir)
		self.copyRecentFilesIntoCache(transfer_q)
