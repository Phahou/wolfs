#!/usr/bin/env python3
import os

import remote

import faulthandler
faulthandler.enable()

from stat import filemode
from datetime import datetime
fromtimestamp = datetime.fromtimestamp

import sys
from logging import getLogger
log = getLogger(__name__)

from IPython import embed
from util import formatByteSize, Col, MaxPrioQueue, mute_unused
from vfsops import VFSOps
from vfs import FileInfo


class HSMCacheFS(VFSOps):
	enable_writeback_cache = True
	enable_acl = True

	def __init__(self, node: remote.RemoteNode, sourceDir: str, cacheDir: str,
				 metadb=None, logFile=None, noatime=True, maxCacheSizeMB=VFSOps._DEFAULT_CACHE_SIZE ):
		super().__init__(sourceDir, cacheDir, maxCacheSizeMB)
		mute_unused(node, remote, metadb, logFile)
		# unused for now:
		# self.metadb = metadb
		# self.log = logFile
		# self.remote = node

		self.time_attr = 'st_mtime_ns' if noatime else 'st_atime_ns'  # remote has mountopt noatime set?

		# initfs
		transfer_q = self.populate_inode_maps()

		# fetch most recently used until cache is 80% full or no more to fetch necessary
		self.copyRecentFilesIntoCache(transfer_q)


	def populate_inode_maps(self):
		"""
		index the sourceDir filesystem tree
		:param self.time_attr decides if mtime or atime is used
		:return: MaxPrioQueue() with most recently edited / accessed files (atime / mtime)
		"""
		transfer_q = MaxPrioQueue()
		for dirpath, dirnames, filenames in os.walk(self.disk.sourceDir):
			dir_attrs = FileInfo.getattr(path=dirpath)
			# self.print_stat(dir_attrs)

			# atime or mtime
			last_used = getattr(dir_attrs, self.time_attr) // 1_000_000_000
			transfer_q.push_nowait((last_used, (dir_attrs.st_ino, dir_attrs.st_size)))
			self._add_path(dir_attrs.st_ino, dirpath, fromPopulate=True)

			for f in filenames:
				filepath = os.path.join(dirpath, f)
				file_attrs = FileInfo.getattr(path=filepath)
				# self.print_stat(file_attrs)

				last_used = getattr(file_attrs, self.time_attr) // 1_000_000_000
				transfer_q.push_nowait((last_used, (file_attrs.st_ino, file_attrs.st_size)))
				self._add_path(file_attrs.st_ino, filepath, fromPopulate=True)

		return transfer_q

	def print_stat(self, entry):
		"""print all common file attributes"""
		attr_list = [
			'st_ino', 'st_mode', 'st_nlink', 'st_uid', 'st_gid',
			# 'st_rdev', # root device id is really useless tbh
			'st_size', 'st_blocks',
			self.time_attr, 'st_ctime_ns'
		]
		if sys.platform in ['bsd', 'OS X']:
			attr_list.append('st_birthtime_ns')

		attr_sw = {
			'st_mode': lambda x: f'{filemode(x)}',
			'st_size': lambda x: formatByteSize(x),
			'st_blocks': lambda x: f'{x} * 512 B',
			'time_ns': lambda x: f'{datetime.fromtimestamp(x // 1_000_000_000).strftime("%d. %b %Y %H:%M")}'
		}
		for attr in attr_list:
			# TODO: make tabular
			#   probably easy with just printing the it like ls -l
			attr_val = getattr(entry, attr)
			attr_str = f'{Col.BOLD}{attr}: {Col.BC}'
			if attr in attr_sw:
				attr_str += attr_sw[attr](attr_val)
			elif 'time_ns' in attr:
				attr_str += attr_sw['time_ns'](attr_val)
			else:
				attr_str += f'{attr_val}'
			print(f'{attr_str}{Col.END}')

	def copyRecentFilesIntoCache(self, transfer_q: MaxPrioQueue):
		print(Col.b('Transfering files...'))

		date = lambda timestamp: fromtimestamp(timestamp).strftime("%d. %b %Y %H:%M")
		while not transfer_q.empty() and not self.disk.isFull(use_threshold=True):
			timestamp, (inode, size) = transfer_q.pop_nowait()
			path = self._inode_path_map[inode]
			if self.disk.canStore(path):
				dest = self.disk.copyIntoCacheDir(path)
				print(f'{date(timestamp)},({inode}, {formatByteSize(size)}) -> {Col.by(dest)}')
			else:
				print(f'{date(timestamp)},({inode}, {size}) -> {Col.br("nowhere cache is too FULL")}')
				continue

		# print summary
		diskUsage, usedCache, maxCache = self.disk.getCurrentStatus()
		diskUsage = Col.by(f'{diskUsage:.8f}%')
		usedCache = Col.by(f'{Col.BY}{formatByteSize(usedCache)} ')
		maxCache = Col.by(f'{formatByteSize(maxCache)} ')
		copySummary = \
			Col.bw(f'Finished transfering.\nCache is now {diskUsage} ') + Col.bw('full') + \
			Col.bw(f" (used: {usedCache}") + Col.bw(f" / {maxCache}") + Col.bw(")")
		print(copySummary)
