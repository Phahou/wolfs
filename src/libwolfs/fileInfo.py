#!/usr/bin/env python
# job of this module:
#  - data container for files, directories and symbolic links
#  - acurately mirror permissions and metadata of backend

import os
from pathlib import Path

import pyfuse3
from pyfuse3 import FUSEError, EntryAttributes
from typing import Union, Any
import stat as stat_m

class FileInfo:

	def __init__(self, fileAttrs: EntryAttributes) -> None:
		self.entry: EntryAttributes = fileAttrs

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
		# st_ino can't be set here as we don't have access to InodeTranslator
		# but ino == 0 is either way an error as we begin counting at 1 in our table
		# so if there is a slipup it will be noticed immediately

		return entry

	@staticmethod
	def setattr(attr: pyfuse3.EntryAttributes, fields: pyfuse3.SetattrFields, path_or_fh: Union[Path, int], ctx: pyfuse3.RequestContext) -> None:
		# We use the f* functions if possible so that we can handle
		# a setattr() call for an inode without associated directory
		# handle.
		fh_isPath = isinstance(path_or_fh, Path)
		if fh_isPath:
			truncate = os.truncate
			chmod = os.chmod
			chown: Any = os.chown
			stat = os.lstat
		else:
			truncate = os.ftruncate
			chmod = os.fchmod
			chown = os.fchown
			stat = os.fstat
		try:
			if fields.update_size:
				truncate(path_or_fh, attr.st_size)

			if fields.update_mode:
				# Under Linux, chmod always resolves symlinks, so we should
				# actually never get a setattr() request for a symbolic
				# link.
				assert not stat_m.S_ISLNK(attr.st_mode)
				chmod(path_or_fh, stat_m.S_IMODE(attr.st_mode))

			if fields.update_uid:
				chown(path_or_fh, attr.st_uid, -1, follow_symlinks=False)

			if fields.update_gid:
				chown(path_or_fh, -1, attr.st_gid, follow_symlinks=False)

			if fields.update_atime and fields.update_mtime:
				if fh_isPath:
					os.utime(path_or_fh, None, follow_symlinks=False,
							 ns=(attr.st_atime_ns, attr.st_mtime_ns))
				else:
					os.utime(path_or_fh, None,
							 ns=(attr.st_atime_ns, attr.st_mtime_ns))
			elif fields.update_atime or fields.update_mtime:
				# We can only set both values, so we first need to retrieve the
				# one that we shouldn't be changing.
				oldstat = stat(path_or_fh)
				if not fields.update_atime:
					attr.st_atime_ns = oldstat.st_atime_ns
				else:
					attr.st_mtime_ns = oldstat.st_mtime_ns
				if fh_isPath:
					os.utime(path_or_fh, None, follow_symlinks=False,
							 ns=(attr.st_atime_ns, attr.st_mtime_ns))
				else:
					os.utime(path_or_fh, None,
							 ns=(attr.st_atime_ns, attr.st_mtime_ns))

		except OSError as exc:
			raise FUSEError(exc.errno)


class listset(list):
	# for checking of something got added twice which is absolutely wrong
	def append(self, item: Any) -> None:
		assert item not in self
		super(listset, self).append(item)

	def __add__(self, other):
		assert other not in self
		super(listset, self).__add__(other)

class DirInfo(FileInfo):
	def __init__(self, fileAttrs: EntryAttributes, child_inodes: list[int]) -> None:
		super().__init__(fileAttrs)
		self.children: listset[int] = listset(child_inodes)

	def __str__(self) -> str:
		return f'childs:{self.children}'
