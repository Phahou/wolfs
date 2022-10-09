#!/usr/bin/env python
# type: ignore

from src.libwolfs.vfs import VFS, MountFSDirectoryInfo
from test.common import create_mount_info
from random import randint
from pyfuse3 import EntryAttributes
from test.util import name_generator
import pytest

###################################################
class TestVFS:
	vfs: VFS
	mount_info: MountFSDirectoryInfo

	# small values as we don't need large ones for testing it works the same
	MIN_INO: int = 2
	MAX_INO: int = 100

	def rand_ino(self):
		return randint(self.MIN_INO, self.MAX_INO)

	@classmethod
	def setup_method(cls) -> None:
		cls.mount_info = create_mount_info()
		cls.vfs = VFS(cls.mount_info)

	@classmethod
	def teardown_method(cls) -> None:
		del cls.mount_info
		del cls.vfs

	def test_add_path_same_ino(self):
		entry = EntryAttributes()
		entry.st_ino = self.rand_ino()
		with pytest.raises(AssertionError):
			self.vfs.add_path(entry.st_ino + 1, name_generator(), entry)

	def test_add_path_add_ino(self):
		entry = EntryAttributes()
		entry.st_ino = self.rand_ino()
		inode: int = entry.st_ino
		self.vfs.add_path(inode, name_generator(), entry)
		assert self.vfs.inode_path_map[inode].entry == entry
		assert self.vfs._lookup_cnt[inode] == 1

	def test_add_path_confirm_lookup_increase(self):
		inode: int = self.rand_ino()
		lkup = self.vfs._lookup_cnt[inode]
		entry = EntryAttributes()
		entry.st_ino = inode
		self.vfs.add_path(inode, name_generator(), entry)
		assert lkup + 1 == self.vfs._lookup_cnt[inode]

	@pytest.mark.skip
	def test_addDirectory(self):
		pass

	@pytest.mark.skip
	def test_addFilePath(self):
		pass
