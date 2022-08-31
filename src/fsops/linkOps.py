import errno
from pyfuse3 import FUSEError, RequestContext, EntryAttributes
from os import fsencode, fsdecode
import os
from pathlib import Path
from src.fsops.xattrs import XAttrsOps
import logging
log = logging.getLogger(__name__)
# pretty much only dead code for now, but it will be used later on when the file system is a bit more
# stable and needs  for example the extended xattrs funcs
class LinkOps(XAttrsOps):
	# link methods (do not use)
	# =========================

	async def readlink(self, inode: int, ctx: RequestContext) -> bytes:
		raise FUSEError(errno.ENOSYS)
		# for reading softlinks
		path: Path = self.vfs.inode_to_cpath(inode)
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
		parent: Path = self.vfs.inode_to_cpath(new_inode_p)
		path: str = os.path.join(parent, new_name)
		try:
			os.link(self.vfs.inode_to_cpath(inode), path, follow_symlinks=False)
		except OSError as exc:
			raise FUSEError(exc.errno)
		# self.vfs.add_path(inode, path)
		return await self.getattr(inode)

	async def symlink(self, inode_p: int, name: str, target: str, ctx: RequestContext) -> EntryAttributes:
		raise FUSEError(errno.ENOSYS)

		name = fsdecode(name)
		target = fsdecode(target)
		parent: Path = self.vfs.inode_to_cpath(inode_p)
		path: str = os.path.join(parent, name)
		try:
			os.symlink(target, path)
			os.chown(path, ctx.uid, ctx.gid, follow_symlinks=False)
		except OSError as exc:
			raise FUSEError(exc.errno)
		#stat = os.lstat(path)
		ino = self.disk.trans.path_to_ino(path)
		lkup = self.vfs._lookup_cnt[ino]
		self.vfs.add_path(ino, path)

		result = await self.getattr(stat.st_ino)

		# post conditions:
		assert self.vfs.getInodeOf(name, inode_p) > 0, "Symbol link named *name* shall be in *inode_p*"
		assert self.readlink(ino, None) == target,  "Symbol link shall point to *target*"
		assert isinstance(result, EntryAttributes), "Return type shall be `EntryAttributes`"
		assert lkup + 1 == self.vfs._lookup_cnt[ino], "On Sucess: Lookup Count shall increase by 1"

		return result
