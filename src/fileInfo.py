import os
from pathlib import Path
from pyfuse3 import FUSEError, EntryAttributes
from typing import Union, Optional

class FileInfo:

	def __init__(self, src: Path, cache: Path, fileAttrs: EntryAttributes) -> None:
		self.src: Union[Path, set[Path]] = Path(src)
		self.cache: Union[Path, set[Path]] = Path(cache)
		self.entry: EntryAttributes = fileAttrs

	def __str__(self) -> str:
		return f'src:{self.src} | cache:{self.cache}'

	@staticmethod
	def getattr(path: Union[str, Path] = None, fd: int = None) -> EntryAttributes:
		assert fd is None or path is None
		assert not (fd is None and path is None)
		try:
			if fd is None:  # get inode attr
				stat = os.lstat(path.__str__())
			else:
				stat = os.fstat(fd)
		except OSError as exc:
			raise FUSEError(exc.errno)

		entry = EntryAttributes()
		# copy file attributes
		for attr in ('st_mode', 'st_nlink', 'st_uid', 'st_gid',
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

class DirInfo(FileInfo):
	def __init__(self, src: Path, cache: Path, fileAttrs: EntryAttributes, child_inodes: list[int]) -> None:
		super().__init__(src, cache, fileAttrs)
		self.children: list[int] = child_inodes

	def __str__(self) -> str:
		return f'src:{self.src} | cache:{self.cache} | childs:{self.children}'
