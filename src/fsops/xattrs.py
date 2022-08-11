#!/usr/bin/env python
from src.fsops.vfsops import VFSOps
import errno
from pyfuse3 import FUSEError, RequestContext
import logging
log = logging.getLogger(__name__)

class XAttrsOps(VFSOps):
	async def access(self, inode: int, mode: int, ctx: RequestContext) -> None:
		# for permissions but eh
		log.info('access')
		raise FUSEError(errno.ENOSYS)

	async def setxattr(self, inode: int, name: str, value: str, ctx: RequestContext) -> None:
		raise FUSEError(errno.ENOSYS)

	async def getxattr(self, inode: int, name: str, ctx: RequestContext) -> None:
		raise FUSEError(errno.ENOSYS)

	async def listxattr(self, inode: int, ctx: RequestContext) -> None:
		raise FUSEError(errno.ENOSYS)

	async def removexattr(self, inode: int, name: str, ctx: RequestContext) -> None:
		raise FUSEError(errno.ENOSYS)
