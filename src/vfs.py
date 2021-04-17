#!/usr/bin/python

# suppress 'unused' warnings
from IPython import embed

embed = embed

import os
import pyfuse3
import errno
import stat as stat_m
from pyfuse3 import FUSEError
from collections import defaultdict
from util import Col
import logging
from pathlib import Path

log = logging.getLogger(__name__)
from disk import CachePath
from util import is_type

class FileInfo:
	# this class should hold any file system information like
	# inode, name, path, attributes
	def __init__(self, src: Path, cache: Path, entry=pyfuse3.EntryAttributes(), runstat=False):
		self.src = Path(src)
		self.cache = Path(cache)
		self.inCacheDir = False
		if runstat:
			self.entry = FileInfo.getattr(src)
		else:
			self.entry = entry

	def toCacheInfo(self, path: Path):
		return FileInfo(path, path, self.entry, False)

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
		entry.generation = 0
		entry.entry_timeout = 0
		entry.attr_timeout = 0
		entry.st_blksize = 512
		entry.st_blocks = ((entry.st_size + entry.st_blksize - 1) // entry.st_blksize)

		return entry


########################################################################################################################

# TODO: fill this and use this in VFSOps

class VFS:
	# I need to save all os operations in this so if a os.lstat is called I can pretend I actually know the stuff
	def __init__(self, sourceDir: Path, cacheDir: Path):
		# TODO: combine _inode_path_map and _inode_path_cache
		# why?: It would half the RAM usage on 1_000_000 files from ~300 MB to ~150MB
		# the cost: the copy of EntryAttributes should be free, as for the paths it doesnt make sense
		#           as they would have to be replaced with a cache prefix every write or so and the gc has to keep track of all these tmp references
		#           compromise would be a btree or mini db on disk something like a metafile right
		#           the btree approach would really not cost more than reading the FileInfo directly as both are probably on disk (if not still loaded in ram)
		# =>
		#   1. combine into one dict
		#   2. save src and cache Paths
		#   3. replace new functionality with old one (here)
		#   4. replace internal calls with this classes calls to inode functions in VFSOps

		srcInfo = FileInfo(Path(sourceDir), Path(cacheDir), runstat=True)
		self._inode_path_map = {pyfuse3.ROOT_INODE: srcInfo}
		self._lookup_cnt = defaultdict(lambda: 0)
		self._fd_inode_map = dict()
		self._inode_fd_map = dict()
		self._fd_open_count = dict()
		self._entry = dict()

	# for better readability
	# def inCache(self, item):
	#	return item in self._inode_path_cache

	# def inRemote(self, item):
	#	return item in self._inode_path_map

	def already_open(self, inode):
		return inode in self._inode_fd_map

	# ==============
	# inode handling
	# ==============

	def _inode_to_path(self, inode):
		"""
		simply maps inodes to paths
		raises errno.ENOENT if not in map -> no such file or directory
		"""
		# check cache if not in cache raise error as we indexed everything from sourceDir in __init__
		# availability is a different matter we simply check if the file exists at all or not
		try:
			val = self._inode_path_map[inode].cache
		except KeyError:
			raise FUSEError(errno.ENOENT)  # no such file or directory

		if isinstance(val, set):
			# In case of hardlinks, pick any path
			val = next(iter(val))
		log.debug(Col.bg(f'_inode_to_path: {inode} -> {val}'))
		return val

	def _add_path(self, inode, path, fromPopulate=False):
		if fromPopulate:
			print(f'{Col.BC}_add_path: {Col.by(f"{inode} -> {path}")}')
		# log.debug('_add_path for %d, %s', inode, path)
		self._lookup_cnt[inode] += 1

		# With hardlinks, one inode may map to multiple paths.
		src_p, cache_p = CachePath.toSrcPath(path), CachePath.toCachePath(path)
		if inode not in self._inode_path_map:
			self._inode_path_map[inode] = FileInfo(src_p, cache_p)
			return

		# generate hardlink from path as inode is already in map
		# as we need to generate it it must be fetched from source
		info = self._inode_path_map[inode]
		if is_type(set, [info.src, info.cache]):
			info.src.add(src_p)
			info.cache.add(cache_p)
		elif info.src != src_p and info.cache != cache_p:
			self._inode_path_map[inode].src = {src_p, info.src}
			self._inode_path_map[inode].cache = {path, info.cache}

	# ============
	# attr methods
	# ============

	async def setattr(self, inode, attr, fields, fh, ctx):
		# We use the f* functions if possible so that we can handle
		# a setattr() call for an inode without associated directory
		# handle.
		if fh is None:
			path_or_fh = self._inode_to_path(inode)
			truncate = os.truncate
			chmod = os.chmod
			chown = os.chown
			stat = os.lstat
		else:
			path_or_fh = fh
			truncate = os.ftruncate
			chmod = os.fchmod
			chown = os.fchown
			stat = os.fstat

		try:
			if fields.update_size:
				truncate(path_or_fh, attr.st_size)

			if fields.update_mode:
				# Under Linux, chmod always resolves symlinks so we should
				# actually never get a setattr() request for a symbolic
				# link.
				assert not stat_m.S_ISLNK(attr.st_mode)
				chmod(path_or_fh, stat_m.S_IMODE(attr.st_mode))

			if fields.update_uid:
				chown(path_or_fh, attr.st_uid, -1, follow_symlinks=False)

			if fields.update_gid:
				chown(path_or_fh, -1, attr.st_gid, follow_symlinks=False)

			if fields.update_atime and fields.update_mtime:
				if fh is None:
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
				if fh is None:
					os.utime(path_or_fh, None, follow_symlinks=False,
							 ns=(attr.st_atime_ns, attr.st_mtime_ns))
				else:
					os.utime(path_or_fh, None,
							 ns=(attr.st_atime_ns, attr.st_mtime_ns))

		except OSError as exc:
			raise FUSEError(exc.errno)

		return await self.getattr(inode)

	async def getattr(self, inode, ctx=None):
		if self.already_open(inode):  # if isOpened(inode):
			return FileInfo.getattr(fd=self._inode_fd_map[inode])
		else:
			return FileInfo.getattr(path=self._inode_to_path(inode))
