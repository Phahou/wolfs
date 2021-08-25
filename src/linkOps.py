import errno
from pyfuse3 import FUSEError
from vfsops import VFSOps
from os import fsencode, fsdecode

class AdditionalOps(VFSOps):

	async def access(self, inode, mode, ctx):
		# for permissions but eh
		raise FUSEError(errno.ENOSYS)

	# xattr methods
	# =============

	async def setxattr(self, inode, name, value, ctx):
		raise FUSEError(errno.ENOSYS)

	async def getxattr(self, inode, name, ctx):
		raise FUSEError(errno.ENOSYS)

	async def listxattr(self, inode, ctx):
		raise FUSEError(errno.ENOSYS)

	async def removexattr(self, inode, name, ctx):
		raise FUSEError(errno.ENOSYS)

	# =====================================================================================================================

	# link methods (do not use)
	# =========================

	async def readlink(self, inode, ctx):
		raise FUSEError(errno.ENOSYS)
		path = self.vfs.inode_to_cpath(inode)
		try:
			target = os.readlink(path)
		except OSError as exc:
			raise FUSEError(exc.errno)
		return fsencode(target)

	async def link(self, inode, new_inode_p, new_name, ctx):
		raise FUSEError(errno.ENOSYS)
		# hardlink
		new_name = fsdecode(new_name)
		parent = self.vfs.inode_to_cpath(new_inode_p)
		path = os.path.join(parent, new_name)
		try:
			os.link(self.vfs.inode_to_cpath(inode), path, follow_symlinks=False)
		except OSError as exc:
			raise FUSEError(exc.errno)
		self.vfs.add_path(inode, path)
		return await self.getattr(inode)

	async def symlink(self, inode_p, name, target, ctx):
		raise FUSEError(errno.ENOSYS)
		name = fsdecode(name)
		target = fsdecode(target)
		parent = self.vfs.inode_to_cpath(inode_p)
		path = os.path.join(parent, name)
		try:
			os.symlink(target, path)
			os.chown(path, ctx.uid, ctx.gid, follow_symlinks=False)
		except OSError as exc:
			raise FUSEError(exc.errno)
		stat = os.lstat(path)
		self.vfs.add_path(stat.st_ino, path)
		return await self.getattr(stat.st_ino)
