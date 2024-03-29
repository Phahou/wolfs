import errno
from pyfuse3 import FUSEError, RequestContext, EntryAttributes
from os import fsencode, fsdecode
import os
from pathlib import Path
from src.fsops.xattrs import XAttrsOps
import logging
import stat
log = logging.getLogger(__name__)
# pretty much only dead code for now, but it will be used later on when the file system is a bit more
# stable and needs  for example the extended xattrs funcs
class LinkOps(XAttrsOps):
	# link methods (do not use)
	# =========================

	async def readlink(self, inode: int, ctx: RequestContext) -> bytes:
		raise FUSEError(errno.ENOSYS)
		# for reading softlinks
		path: Path = self.disk.ino_toTmp(inode)
		try:
			target = os.readlink(path)
		except OSError as exc:
			raise FUSEError(exc.errno)
		return fsencode(target)

	async def link(self, inode: int, new_inode_p: int, new_name: str, ctx: RequestContext) -> EntryAttributes:
		raise FUSEError(errno.ENOSYS)
		# hardlink
		log.info()
		new_name = fsdecode(new_name)
		parent: Path = self.disk.ino_toTmp(new_inode_p)
		path: str = os.path.join(parent, new_name)
		try:
			os.link(self.disk.ino_toTmp(inode), path, follow_symlinks=False)
		except OSError as exc:
			raise FUSEError(exc.errno)
		# self.vfs.add_path(inode, path)
		return await self.getattr(inode)

	async def symlink(self, inode_p: int, name: str, target: str, ctx: RequestContext) -> EntryAttributes:
		raise FUSEError(errno.ENOSYS)
		parent_path: str = self.disk.ino_to_rpath(inode_p)
		assert not isinstance(parent_path, set), f"Something went horrendously wrong. A directory and has 2 hardlink paths: {parent_path}"
		name: str = fsdecode(name)
		symlink_path: str = parent_path + "/" + name
		ino: int = self.disk.path_to_ino(symlink_path)
		try:
			os.symlink(target, symlink_path)
			os.chown(symlink_path, ctx.uid, ctx.gid, follow_symlinks=False)
		except OSError as exc:
			raise FUSEError(exc.errno)
		entry = self.getattr(ino)
		self.vfs.add_Child(inode_p, ino, symlink_path, entry)

		# for softlinks

		# target = fsdecode(target)
		# parent: Path = self.vfs.inode_to_cpath(inode_p)
		# path: str = os.path.join(parent, name)
		#stat = os.lstat(path)
		ino = self.disk.path_to_ino(path)
		lkup = self.vfs._lookup_cnt[ino]
		self.vfs.add_path(ino, path, entry)

		result = await self.getattr(stat.st_ino)

		# post conditions:
		assert self.disk.path_to_ino(parent_path) == inode_p, "Symbol link named *name* shall be in *inode_p*"
		assert self.readlink(ino, None) == target,  "Symbol link shall point to *target*"
		assert isinstance(result, EntryAttributes), "Return type shall be `EntryAttributes`"
		assert lkup + 1 == self.vfs._lookup_cnt[ino], "On Sucess: Lookup Count shall increase by 1"

		return result
