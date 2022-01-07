# directory methods
# =================
from src.xattrs import XAttrsOps
from vfsops import VFSOps, log
import os
import os.path
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
		# in cache:
		#		mkdir normally, update DirInfo of parent and create a DirInfo for new ino
		# in src:
		#		log that it needs to be made if it was possible in cache
		# might cause problems:
		# 	- no disk space in:
		# 		- src   but in cache
		#		- cache but in src
		#		- cache & src
		#
		# softlinks dont need to be regarded as they dont come up here
		#
		# exec:
		#	1. get info of inode_p and stuff
		#	2. check if enough disk space is there in cache and src
		#		if not for cache & src:   raise FUSEError(errno.ENOSPC)
		#	3. try to mkdir
		#		might fail due to permissions
		#	4. update DirInfo of inode_p & new inode, track...
		#	5. log to journal and sync later (assumption): no one modifies the directory on the backend
		#	6. done
		def validity_check():

			# abort if directory already exists (we have to check this virtually
			# as Path.exists() might say no although it already exists in the src )
			if self.vfs.getInodeOf(cpath, inode_p):
				log.warning(f"Tried to make a directory that already exists:"
							f"  mkdir({parent_path},{name},{hex(mode)})")
				raise FUSEError(errno.EEXIST)

			# 2. check for disk space in _cache_
			# TODO: checks for cache disk but not src
			#		might need another module as we arent currently tracking free space on the backing store
			if not self.disk.canReserve(self.disk.MIN_DIR_SIZE):
				raise FUSEError(errno.ENOSPC)


		log.info(f" {Col(name)} in {Col(inode_p)} with mode {Col.file(mode & ctx.umask)}")

		# 1. get info of inode_p and stuff
		parent_path = self.vfs.inode_to_cpath(inode_p)
		cpath = os.path.join(parent_path, fsdecode(name))
		validity_check()

		try:
			# can succeed as dir might not be present in cache
			os.mkdir(cpath, mode=(mode & ~ctx.umask))
			os.chown(cpath, ctx.uid, ctx.gid)
		except OSError as exc:
			raise FUSEError(exc.errno)

		# update book-keeping
		attr = FileInfo.getattr(path=cpath)
		attr.st_ino = self.disk.track(cpath)
		self.vfs.addFilePath(inode_p, attr.st_ino, cpath, attr)
		self.journal.mkdir(attr.st_ino, cpath, mode)

		return attr

	async def rmdir(self, inode_p: int, name: str, ctx: pyfuse3.RequestContext) -> None:
		# TODO: log on success
		#       die.net: Upon successful completion, the rmdir() function shall mark for update the st_ctime and st_mtime fields of the parent directory.
		#       -> update entries

		inode_p = self.mnt_ino_translation(inode_p)

		parent = self.vfs.inode_to_cpath(inode_p)
		cpath = os.path.join(parent, fsdecode(name))
		inode = self.path_to_ino(cpath)
		log.info(f"{Col(inode)}({Col(cpath)}) in {Col(inode_p)}({Col(parent)})")

		# check if cpath is softlink into a flag (hardlinks to dirs dont exist)

		# check if cpath is present in tmp dir
		# 	yes -> use a native rmdir
		#   no  ->	skip rmdir,
		#   		check if dir is present in backend -> oh its more complex than I thought
		#   		-> yes:	would it be possible to	remove the directory with current permissions and so on ?
		#				-> 	yes: do a "virtual" rmdir
		#					another way would be: mkdir with saved DirInfo and then
		#					deleting it but that doesnt make much sense at all
		#			-> no:
		#				erno.ENOEXT (dir doesnnot exist)
		#

		# update parent inode according to die.net
		#   either way -> update dirinfo of inode_p and delete path from dirinfo of path
		#		if dirinfo has no more links then delete inode of path too as (hardlink case)

		# no exceptions: log in journal that directory has to be removed later
		# exception: 	 log nothing return
		if os.path.exists(cpath):
			try:
				os.rmdir(cpath)
			except OSError as exc:
				raise FUSEError(exc.errno)
		else:
			assert False, "cpath search not implemented in for backend"
			pass
			#if self.disk.isInBackend(inode):

		self.journal.rmdir(inode, cpath)
		if self.vfs.inLookupCnt(inode):
			self._forget_path(inode, cpath)

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

			return sorted(entries) if len(entries) > 0 else None

		if off == 0:
			path = self.vfs.inode_to_cpath(inode)
			log.info(f'{Col(path)}')

			# for posix compatibility we freeze the directory entries returned between
			# each readdir cycle. This ensures that we dont skip any entries or report them twice
			# as required by pyfuse. This doesnt mean opening the same directory twice wouldnt
			# show the same results by different processes
			if freezed_dirents := freeze_dirents():
				self.freezed_dirents[inode] = freezed_dirents
		s_entries = self.freezed_dirents.get(inode)

		if s_entries is None:
			return

		# skip last run as nothing will be returned either way
		elif off != 0 and off == s_entries[-1][0]:
			del self.freezed_dirents[inode]
			return

		i = 0
		log.debug('  %d entries left, starting at offset/ino %d', len(s_entries), off)
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