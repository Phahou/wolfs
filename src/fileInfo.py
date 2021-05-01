import os
from pathlib import Path

import pyfuse3
from pyfuse3 import FUSEError


class FileInfo:
	"""
	this class should hold any file system information like
	path, attributes, child_inodes
	"""

	def __init__(self, src: Path, cache: Path, fileAttrs: pyfuse3.EntryAttributes, child_inodes=None):
		self.src = Path(src)
		self.cache = Path(cache)
		# use None as it only uses 2 bytes instead of 5 per file and most files arent folders
		self._childs = child_inodes
		self.entry = fileAttrs
		self.write_ops = None

	def __str__(self):
		return f'src:{self.src} | cache:{self.cache} | childs:{self._childs}'

	def updateEntry(self, path=None, fd=None, entry=None):
		self.entry = entry if entry else FileInfo.getattr(path, fd)

	@staticmethod
	def getattr(path=None, fd=None):
		assert fd is None or path is None
		assert not (fd is None and path is None)
		try:
			if fd is None:  # get inode attr
				stat = os.lstat(path.__str__())
			else:
				stat = os.fstat(fd)
		except OSError as exc:
			raise FUSEError(exc.errno)

		entry = pyfuse3.EntryAttributes()
		# copy file attributes
		for attr in ('st_ino', 'st_mode', 'st_nlink', 'st_uid', 'st_gid',
					 'st_rdev', 'st_size', 'st_atime_ns', 'st_mtime_ns',
					 'st_ctime_ns'):
			setattr(entry, attr, getattr(stat, attr))  # more general way of entry.'attr' = stat.'attr'
		# TODO: probably needs a rework after the NFS is mounted
		# 		the inode generation in nfs is not stable after a server restart
		# src:  https://stackoverflow.com/questions/11071996/what-are-inode-generation-numbers
		entry.generation = 0
		# validity of this entry to the kernel
		# doc-url: https://www.fsl.cs.stonybrook.edu/docs/fuse/fuse-article-appendices.html
		entry.entry_timeout = float('inf')
		entry.attr_timeout = float('inf')

		entry.st_blksize = 512
		entry.st_blocks = ((entry.st_size + entry.st_blksize - 1) // entry.st_blksize)

		return entry
