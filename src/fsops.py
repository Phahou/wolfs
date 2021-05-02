#!/usr/bin/env python3

# suppress 'unused' warnings
from IPython import embed
embed = embed

import os
import remote
import faulthandler

faulthandler.enable()

from pathlib import Path
from logging import getLogger

log = getLogger(__name__)

from util import formatByteSize, Col, MaxPrioQueue, mute_unused
from vfsops import VFSOps
from fileInfo import FileInfo
from errors import NotEnoughSpaceError
import pickle


def save_obj(obj, name):
	with open(name, 'wb+') as f:
		pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name):
	with open(name, 'rb') as f:
		return pickle.load(f)


class HSMCacheFS(VFSOps):
	enable_writeback_cache = True
	enable_acl = True

	def __init__(self, node: remote.RemoteNode, sourceDir: str, cacheDir: str,
				 metadb=None, logFile=None, noatime=True, maxCacheSizeMB=VFSOps._DEFAULT_CACHE_SIZE):
		super().__init__(node, Path(sourceDir), Path(cacheDir), maxCacheSizeMB, noatime)
		mute_unused(metadb, logFile)
		# unused for now:
		if self.remote.isMounted():
			transfer_q = self.populate_inode_maps(self.disk.sourceDir)
			self.copyRecentFilesIntoCache(transfer_q)
		else:
			# remote is offline our best guess is to believe that the cache is up to date
			# todo: set to poll is remote is mounted and replace metafile if so
			self.restoreInternalState(Path(metadb))

		# self.metadb = metadb
		# self.log = logFile
		# self.remote = node

		# initfs
		transfer_q = self.populate_inode_maps(self.disk.sourceDir)
		save_obj(self.vfs._inode_path_map, metadb)

		# fetch most recently used until cache is full to defined threshold or no more to fetch necessary
		self.copyRecentFilesIntoCache(transfer_q)

	def populate_inode_maps(self, root: str):
		"""
		index the sourceDir filesystem tree
		:param root: root directory to add to filesystem to
		:param self.time_attr decides if mtime or atime is used
		:return: MaxPrioQueue() with most recently edited / accessed files (atime / mtime)
		"""

		def print_progress(func_str, st_ino, path):
			if st_ino in range(1_000, 1_000_000, 1_000):
				log.debug(f'{Col.BC}{func_str}: {Col.by(f"{Col.bg(st_ino)}-> {path}")}')

		transfer_q = MaxPrioQueue()

		def push_to_queue(dir_attrs):
			last_used = getattr(dir_attrs, self.disk.time_attr) // self.disk.__NANOSEC_PER_SEC__
			transfer_q.push_nowait((last_used, (dir_attrs.st_ino, dir_attrs.st_size)))

		for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
			dir_attrs = FileInfo.getattr(path=dirpath)
			push_to_queue(dir_attrs)

			child_inodes = []
			for d in dirnames:
				# only add child inodes here the subdirs will be walked through
				subdir_path = os.path.join(dirpath, d)
				child_inodes.append(FileInfo.getattr(subdir_path).st_ino)

			for f in filenames:
				filepath = os.path.join(dirpath, f)

				file_attrs = FileInfo.getattr(path=filepath)
				push_to_queue(file_attrs)

				self.vfs.add_path(file_attrs.st_ino, filepath, file_attrs)
				print_progress('add_path', file_attrs.st_ino, filepath)
				child_inodes.append(file_attrs.st_ino)

			self.vfs.add_Directory(dir_attrs.st_ino, dirpath, dir_attrs, child_inodes)
			print_progress('add_Directory', dir_attrs.st_ino, dirpath)

		return transfer_q

	def copyRecentFilesIntoCache(self, transfer_q: MaxPrioQueue):
		print(Col.b('Transfering files...'))
		while not transfer_q.empty() and not self.disk.isFull(use_threshold=True):
			timestamp, (inode, file_size) = transfer_q.pop_nowait()
			path = self.vfs._inode_path_map[inode].src

			# skip symbolic links for now
			if os.path.islink(path):
				continue

			try:
				dest = self.disk.cp2Cache(path)
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
		diskUsage = Col.by(f'{diskUsage:.8f}%')
		usedCache = Col.by(f'{Col.BY}{formatByteSize(usedCache)} ')
		maxCache = Col.by(f'{formatByteSize(maxCache)} ')
		copySummary = \
			Col.bw(f'Finished transfering {self.disk.in_cache.qsize()} elements.\nCache is now {diskUsage} ') + Col.bw(
				'full') + \
			Col.bw(f" (used: {usedCache}") + Col.bw(f" / {maxCache}") + Col.bw(")")
		print(copySummary)
