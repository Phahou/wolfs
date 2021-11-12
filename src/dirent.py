# directory methods
# =================
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
from util import __functionName__


class DirentOps(VFSOps):

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
		path = self.vfs.inode_to_cpath(inode)
		log.info(f'{__functionName__(self)} {Col.path(path)}')
		await self.__readdir(inode, off, token)

	async def __readdir(self, inode: int, off: int, token: pyfuse3.ReaddirToken) -> None:
		entries: Union[tuple, list[tuple[int, str, pyfuse3.EntryAttributes]]] = []
		# copy attributes from src Filesystem
		dirent = self.vfs.inode_path_map[inode]
		if isinstance(dirent, DirInfo):
			childs = dirent.children
			log.debug(__functionName__(self) + f' searching through {Col.inode(childs)}')
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
			log.debug(f"{__functionName__(self)} {dirent} is of type FileInfo -> readdir_reply call")
			entries = ()

		s_entries = sorted(entries)

		log.debug('  read %d entries, starting at %d', len(entries), off)
		# This is not fully posix compatible. If there are hardlinks
		# (two names with the same inode), we don't have a unique
		# offset to start in between them. Note that we cannot simply
		# count entries, because then we would skip over entries
		# (or return them more than once) if the number of directory
		# entries changes between two calls to readdir().
		for (ino, name, attr) in s_entries:
			if ino <= off:
				continue
			log.debug(f"    {Col.inode(ino)}, {Col.file(name)}")
			if pyfuse3.readdir_reply(token, fsencode(name), attr, ino):
				self.vfs._lookup_cnt[attr.st_ino] += 1
			else:
				break
