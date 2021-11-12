# directory methods
# =================
from src.xattrs import XAttrsOps
from vfsops import VFSOps, log
import os
import pyfuse3
import errno
from pyfuse3 import FUSEError
from os import fsencode, fsdecode
from util import Col
from fileInfo import FileInfo, DirInfo
from typing import Final, Union, cast
from remote import RemoteNode  # type: ignore

class DirentOps(XAttrsOps):

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		# used to temporarily store directory entries while a readdir call is performed
		self.freezed_dirents: dict[int: [int]] = dict()

	async def mkdir(self, inode_p: int, name: str, mode: int, ctx: pyfuse3.RequestContext) -> pyfuse3.EntryAttributes:
		raise FUSEError(errno.ENOSYS)
		log.info(f" {Col(name)} in {Col(inode_p)} with mode {Col.file(mode & ctx.umask)}")
		# works but isnt snappy (folder is only shown after reentering it in thunar)
		path = os.path.join(self.vfs.inode_to_cpath(inode_p), fsdecode(name))
		if inode := self.vfs.getInodeOf(path, inode_p):
			raise FUSEError(errno.EEXIST)
		try:
			# can succeed as dir might not be present in cache
			os.mkdir(path, mode=(mode & ~ctx.umask))
			os.chown(path, ctx.uid, ctx.gid)
		except OSError as exc:
			raise FUSEError(exc.errno)
		attr = FileInfo.getattr(path=path)
		attr.st_ino = self.disk.track(path)
		self.vfs.addFilePath(inode_p, attr.st_ino, path, attr)
		self.journal.mkdir(attr.st_ino, path, mode)
		return attr

	async def rmdir(self, inode_p: int, name: str, ctx: pyfuse3.RequestContext) -> None:
		# TODO: log on success
		#       die.net: Upon successful completion, the rmdir() function shall mark for update the st_ctime and st_mtime fields of the parent directory.
		#       -> update entries
		raise FUSEError(errno.ENOSYS)
		inode_p = self.mnt_ino_translation(inode_p)

		name = fsdecode(name)
		parent = self.vfs.inode_to_cpath(inode_p)
		path = os.path.join(parent, name)
		inode = self.path_to_ino(path)
		log.info(f"{Col(inode)}({Col(path)}) in {Col(inode_p)}({Col(parent)})")
		self.fetchFile(inode)
		try:
			os.rmdir(path)
		except OSError as exc:
			raise FUSEError(exc.errno)

		self.journal.rmdir(inode, path)
		if self.vfs.inLookupCnt(inode):
			self._forget_path(inode, path)

	async def opendir(self, inode: int, ctx: pyfuse3.RequestContext) -> int:
		inode = self.mnt_ino_translation(inode)
		dirent: DirInfo = cast(DirInfo, self.vfs.inode_path_map[inode])
		log.info(f"{Col.path(self.vfs.inode_to_cpath(inode))} contains: {Col(dirent.children)}")
		# ctx contains gid, uid, pid and umask
		return inode

	async def readdir(self, inode: int, off: int, token: pyfuse3.ReaddirToken) -> None:
		def freeze_dirents():
			entries: Union[tuple, list[tuple[int, str, pyfuse3.EntryAttributes]]] = []
			# copy attributes from src Filesystem
			dirent = self.vfs.inode_path_map[inode]
			if isinstance(dirent, DirInfo):
				childs = dirent.children
				log.debug(f'searching through {Col(childs)}')
				for child_inode in childs:
					try:
						info: Union[FileInfo, DirInfo] = self.vfs.inode_path_map[child_inode]
						# __Very strange__ that `info.entry.st_ino` changes between calls although we dont write to it ?
						# info.entry.st_ino = child_inode
						entries.append((child_inode, info.cache.name, info.entry))  # type: ignore
					except KeyError:
						# TODO: ignore missing symlinks for now
						log.error(f'{Col.BR}Ignored FileInfo of {Col.BG}{child_inode}')

			else:
				log.debug(f"{dirent} is of type FileInfo -> readdir_reply call")
				entries = ()

			return sorted(entries)

		if off == 0:
			path = self.vfs.inode_to_cpath(inode)
			log.info(f'{Col(path)}')

			# for posix compatibility we freeze the directory entries returned between
			# each readdir cycle. This ensures that we dont skip any entries or report them twice
			# as required by pyfuse. This doesnt mean opening the same directory twice wouldnt
			# show the same results by different processes
			self.freezed_dirents[inode] = freeze_dirents()
		s_entries = self.freezed_dirents[inode]

		# skip last run as nothing will be returned either way
		if off != 0 and off == s_entries[-1][0]:
			del self.freezed_dirents[inode]
			return

		i = 0
		log.debug('  %d entries left, starting at ino %d', len(s_entries), off)
		# as we didnt tested posix compatibility yet we keep this warning:
		# 	This is not fully posix compatible. If there are hardlinks
		# 	(two names with the same inode), we don't have a unique
		# 	offset to start in between them. Note that we cannot simply
		# 	count entries, because then we would skip over entries
		# 	(or return them more than once) if the number of directory
		# 	entries changes between two calls to readdir().
		for (ino, name, attr) in s_entries:
			log.debug(f"    {Col.inode(ino)}, {Col.file(name)}")
			if pyfuse3.readdir_reply(token, fsencode(name), attr, ino):
				self.vfs._lookup_cnt[attr.st_ino] += 1
			else:
				self.freezed_dirents[inode] = self.freezed_dirents[inode][i:]
				return
			i += 1

	async def releasedir(self, fh: int) -> None:
		# same as normal release() no more fh are using it
		log.info(f'Released Dir: {Col.path(self.vfs.inode_to_cpath(fh))}')
