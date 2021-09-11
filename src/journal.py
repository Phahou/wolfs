#!/usr/bin/python

# external imports
from pathlib import Path
import os
from enum import Flag, auto
import dataclasses
import logging

import pyfuse3

log = logging.getLogger(__name__)

# custom imports
from disk import Disk
from vfs import VFS
from util import Col, __functionName__
from disk import Path_str
from IPython import embed

embed = embed
from typing import TypeVar, Final
Write_Op = tuple[int, int]
INVALID_VALUE: Final[int] = -1


class File_Ops(Flag):
	CREATE = auto()
	WRITE = auto()
	UNLINK = auto()
	RENAME = auto()
	MKDIR = auto()

@dataclasses.dataclass
class LogEntry:
	op: File_Ops
	inode: int
	path: str
	writes: Write_Op = (INVALID_VALUE, INVALID_VALUE)
	flags: int = INVALID_VALUE
	mode: int = INVALID_VALUE
	path_new: str = ""

class Journal:
	supported_ops: Final = [File_Ops.CREATE, File_Ops.WRITE, File_Ops.UNLINK, File_Ops.MKDIR]
	__EMPTY_FDS = (0, 0)

	def __init__(self, disk: Disk, vfs: VFS, logFile: Path):
		self.disk: Disk = disk
		self.vfs: VFS = vfs
		self.logFile = logFile
		self.__history: list[LogEntry] = []
		self.__inode_dirty_map2: dict[int, bool] = dict()
		self.__last_remote_path: str = ""
		self.__last_fds: Write_Op = Journal.__EMPTY_FDS

	# private api
	# ===========

	def __fsyncFile_with_remote(self, cache_file: str, write_ops: list[Write_Op]) -> None:
		"""
		Syncs `cache_file` with remote by applying `write_ops` to the corresponding remote file
		:param cache_file: cached file to be synced
		:param write_ops: history of write operations to file preceeding last sync
		"""
		remote: Path = self.disk.toSrcPath(cache_file)
		assert cache_file == self.disk.toCachePath(cache_file), "Tried to overwrite cache with remote file"
		assert remote.exists(), "Writing before the file was created ???"

		fd_cache, fd_remote = self.__last_fds
		# only close/write to files if we really need to.
		if self.__last_remote_path != remote:
			if self.__last_fds != Journal.__EMPTY_FDS:
				os.fsync(fd_remote)
				os.close(fd_cache); os.close(fd_remote)
			cache_flags = os.O_RDONLY | os.O_NOATIME
			remote_flags = os.O_RDWR | os.O_NOATIME  # | os.O_DIRECT | os.O_SYNC # apparently we get errno.EINVAL with this
			fd_cache, fd_remote = os.open(cache_file, cache_flags), os.open(remote, remote_flags)
			self.__last_fds = (fd_cache, fd_remote)

		# copy file Contents without truncuation
		for offset, buflen in write_ops:
			os.lseek(fd_cache, offset, os.SEEK_SET)
			os.lseek(fd_remote, offset, os.SEEK_SET)
			buf = os.read(fd_cache, buflen)
			os.write(fd_remote, buf)

		# copy file Attributes (keeps us from logging setattrs too)
		# TODO: could probably just change them when we really need to
		#      like if remote == last entry of history or sth
		#      and somewhere in the self.__last_remote_path is different
		Disk.cpAttrs(cache_file, remote)

	def __replayFile_Op(self, op: File_Ops, src_path: Path, logEntry: LogEntry, i: int) -> int:
		def __unlink(src_path: Path) -> None:
			try:
				os.remove(src_path)
			except IsADirectoryError:
				os.rmdir(src_path)
			except FileNotFoundError:
				if '.Trash' in src_path.__str__():
					log.warning(__functionName__(self, 3) + f"{Col.path(src_path)} not found in Trash -> Ignoring")

		def __mkdir(src_path: Path, logEntry: LogEntry) -> None:
			mode: int = getattr(logEntry, 'mode')
			try:
				os.mkdir(src_path, mode)
			except OSError as exc:
				log.error(exc)
				raise pyfuse3.FUSEError(exc.errno)

		def __create(src_path: Path, logEntry: LogEntry) -> None:
			flags: int = getattr(logEntry, 'flags')
			fd = os.open(src_path, flags)
			os.close(fd)

		def __rename(src_path: Path, logEntry: LogEntry) -> None:
			path_new: Path = self.disk.toSrcPath(getattr(logEntry, 'path_new'))
			os.rename(src_path, path_new)
			self.vfs.inode_path_map[logEntry.inode].src = path_new

		switcher = {
			File_Ops.CREATE: lambda x, y, z: __create(x, y),
			File_Ops.MKDIR:  lambda x, y, z: __mkdir(x, y),
			File_Ops.UNLINK: lambda x, y, z: __unlink(x),
			File_Ops.RENAME: lambda x, y, z: __rename(x, y),
		}
		if op == File_Ops.WRITE:
			# fetch all writes happening directly after this one
			writes: list[Write_Op] = []
			file_path = logEntry.path
			history_iter = iter(self.__history[i:])
			while writeEntry := next(history_iter, None):
				if writeEntry.op != File_Ops.WRITE or writeEntry.path != file_path:
					break
				else:
					writes.append(getattr(writeEntry, 'writes'))
					i += 1
			self.__fsyncFile_with_remote(logEntry.path, writes)
			return i
		else:
			switcher[op](src_path, logEntry, i)
			return i + 1

	def __markDirty(self, inode: int) -> None:
		self.__inode_dirty_map2[inode] = True

	# util funcs
	# ==========

	def flushCompleteJournal(self) -> None:
		# empty out unneeded ops if inodes was deleted unlinked entries
		# Doesnt sort out rewrites of files under a different inode, read / writes will be ok though

		unlink_entries = list(filter(lambda x: x.op == File_Ops.UNLINK, self.__history))
		unlink_inos = list(map(lambda x: x.inode, unlink_entries))

		compacted_history = []
		for logEntry in self.__history:
			if logEntry.inode not in unlink_inos:
				item = logEntry
			else:
				# insert unlink_entry and skip every other entry according to that ino
				try:
					item = unlink_entries[unlink_inos.index(logEntry.inode)]
				except KeyError:
					continue
			compacted_history.append(item)

		len_history = len(compacted_history)
		log.info(f'{Col.BG}Flushing complete Journal: {Col.BY}{len_history}{Col.BG} entries')

		i = 0
		while i < len_history:
			logEntry = compacted_history[i]
			src_path = self.disk.toSrcPath(logEntry.path)
			i = self.__replayFile_Op(logEntry.op, src_path, logEntry, i)
			if i % 25 == 0:
				print(f'Processed {i} items')

		log.info(f'{Col.BW}Finished flushing complete Journal')
		# clean internal buffers
		self.__history = []
		self.__inode_dirty_map2 = {}
		self.__last_fds = Journal.__EMPTY_FDS
		self.__last_remote_path = ""

	def getDirtyPaths(self) -> tuple[list[Path], int]:
		dirty_paths: list[Path] = []
		write_ops_reserved_size: int = 0
		for logEntry in self.__history:
			if logEntry.op != File_Ops.WRITE:
				continue
			(_, bytes_written) = getattr(logEntry, 'writes')
			dirty_paths.append(self.vfs.inode_to_cpath(logEntry.inode))
			write_ops_reserved_size += bytes_written
		return dirty_paths, write_ops_reserved_size

	def isDirty(self, inode: int) -> bool:
		return inode in self.__inode_dirty_map2

	def isCompletelyClean(self) -> bool:
		return self.__inode_dirty_map2 == {}

	# public api
	# ==========

	def create(self, inode: int, path: str, flags: int) -> None:
		self.__markDirty(inode)
		e: LogEntry = LogEntry(File_Ops.CREATE, inode, path)
		e.flags = flags
		self.__history.append(e)

	def write(self, inode: int, offset: int, bytes_written: int) -> None:
		self.__markDirty(inode)
		e: LogEntry = LogEntry(File_Ops.WRITE, inode, self.vfs.inode_to_cpath(inode).__str__())
		e.writes = (offset, bytes_written)
		self.__history.append(e)

	def flush(self, inode: int, fh: int) -> None:
		# TODO: sync up later (timer would probably be the best choice or
		#  		some kind of check if there is almost no space available on underlying cache disk)
		# special case if we enable renaming things:
		# log.warning(f'{__functionName__(self, 2)} Not implemented')
		pass

	def rename(self, inode: int, path_old: str, path_new: str) -> None:
		self.__markDirty(inode)
		e: LogEntry = LogEntry(File_Ops.RENAME, inode, path_old)
		e.path_new = path_new
		self.__history.append(e)

	def unlink(self, inode: int, path: str) -> None:
		self.__markDirty(inode)
		e: LogEntry = LogEntry(File_Ops.UNLINK, inode, path)
		self.__history.append(e)

	def rmdir(self, inode: int, path: str) -> None:
		# same as unlink of a file as non empty dirs would already have raised an error
		self.unlink(inode, path)

	def mkdir(self, inode: int, path: str, mode: int) -> None:
		self.__markDirty(inode)
		e: LogEntry = LogEntry(File_Ops.MKDIR, 0, path)
		e.mode = mode
		self.__history.append(e)
