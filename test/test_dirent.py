#!/usr/bin/env python
# -*- coding: utf-8 -*-
# type: ignore

# test is a bit flaky with segfaults errors
# I don't know if it's my code or the one from some library as pytest
# also throws their errors sometimes in the mix
# they are currently rare enough to ignore though

# internal imports
from wolfs import mountfs
from test.util import rand_string, rmtree, coinflip
from src.fsops.dirent import DirentOps
from src.libwolfs.translator import MountFSDirectoryInfo


# specific test imports
import getpass # check if user is root (NOSPC-Test needs a tmpfs)
from random import randrange # mkdir: simulate random user creation
from pyfuse3 import FUSEError

# boilerplate
from test.common import create_mount_info, micro_sleep
from pathlib import Path
import pytest
import os

# for starting wolfs
from argparse import Namespace
import threading
import subprocess

# globals
FS_SIZE = 16 # tmpfs size in Megabytes

class MountFS(threading.Thread):
	operations: DirentOps
	mount_info: MountFSDirectoryInfo
	DEFAULT_TIMEOUT: float = 0.001 # 1 miliseconds for joining threads

	def __init__(self, mount_info: MountFSDirectoryInfo):
		super().__init__()
		self.mount_info = mount_info
		self.operations = DirentOps(None, self.mount_info, None)
		self.started_fs = False

	def run(self) -> None:
		if not self.started_fs:
			options = Namespace(debug_fuse=False)
			self.started_fs = True
			mountfs(self.operations, options)

	def wait_for_mount(self) -> None:
		timeout_step = 1_000 # microseconds per milisecond
		timeout = 25 # in miliseconds
		while timeout > 0:
			mounted_filesystems = subprocess.check_output(["mount"]).__str__()
			index = mounted_filesystems.find("wolfs on ")
			mounted_filesystems = mounted_filesystems[index:]

			expected = f"wolfs on {self.mount_info.mountDir.__str__()}"
			if expected in mounted_filesystems:
				break
			micro_sleep(timeout_step)
			timeout -= 1
		if timeout <= 0:
			assert False, "Failed initilization of wolfs"

	def cleanup(self, timeout: float = DEFAULT_TIMEOUT) -> None:
		if Path(self.mount_info.mountDir).is_mount():
			os.system(f"fusermount -u -z {self.mount_info.mountDir}")
		for dentry in [self.mount_info.cacheDir, self.mount_info.sourceDir, self.mount_info.mountDir]:
			try:
				rmtree(dentry)
			except FileNotFoundError:
				pass
		self.join(timeout)

def teardown_module(module):
	"""teardown any state that was previously setup with a setup_module method."""
	pass

@pytest.mark.slow
class TestMkdirOps:
	mode: int = 0o700  # user: rwx | group & other have no permissions
	filesystem: MountFS
	mount_info: MountFSDirectoryInfo

	@classmethod
	def setup_method(cls) -> None:
		cls.mount_info = create_mount_info()
		cls.filesystem = MountFS(cls.mount_info)
		cls.filesystem.start()
		cls.filesystem.wait_for_mount()

	@classmethod
	def teardown_method(cls) -> None:
		cls.filesystem.cleanup()

	def normalize_path(self, path: Path | str):
		path: str = self.filesystem.operations.disk.toRoot(path)
		path = path.__str__().replace("//", "/")
		if path[0] == '/':
			return path[1:]

	def test_mkdir_in_root(self):
		mountpoint: Path = self.mount_info.mountDir
		f1 = os.path.join(mountpoint, rand_string())
		f2 = os.path.join(mountpoint, rand_string())
		os.mkdir(f1)
		os.mkdir(f2)
		disk = self.filesystem.operations.disk
		root_dir = list(map(lambda x: '/' + x, os.listdir(mountpoint)))
		assert set(root_dir) == {disk.toRoot(f1), disk.toRoot(f2)}
		ino_path_map = self.filesystem.operations.vfs.inode_path_map
		assert ino_path_map != dict()

		for d in root_dir:
			os.rmdir(disk.toMnt(d))
			assert d not in os.listdir(mountpoint)
			assert len(os.listdir()) != 0
		assert [] == os.listdir(mountpoint)

	def test_mkdir_in_subfolder(self):
		rdir = os.path.join(self.mount_info.mountDir, rand_string())
		dirname = rand_string()
		subdir = os.path.join(rdir, dirname)

		os.mkdir(rdir)
		os.mkdir(subdir)
		p_subdir = Path(subdir)
		assert self.normalize_path(rdir) in os.listdir(self.mount_info.mountDir)
		assert p_subdir.exists() and p_subdir.is_dir()
		assert os.listdir(subdir) == []
		assert os.listdir(rdir) == [dirname]
		prev_folder = subdir

		# do some mkdirs randomly to make sure we don't just have specialized subdir tests
		for i in range(randrange(2, 20)):
			dirpath = os.path.join(prev_folder, rand_string())
			os.mkdir(dirpath)
			if coinflip():
				prev_folder = dirpath

		# cleanup
		rmtree(subdir)
		assert os.listdir(rdir) == []
		os.rmdir(rdir)

		# assert self.normalize_path(subdir) in os.listdir(subdir_path)

	def test_mkdir_already_exists(self):
		f1 = os.path.join(self.mount_info.mountDir, rand_string())
		os.mkdir(f1)
		with pytest.raises(FileExistsError) as e:
			os.mkdir(f1)

	def test_mkdir_missing_folder_in_between(self):
		f1 = os.path.join(self.mount_info.mountDir, rand_string())
		f2 = os.path.join(f1, rand_string())
		with pytest.raises(FileNotFoundError) as e:
			os.mkdir(f2)

	# setting up pycharm run 'sudo python "$@"' without asking for a password:
	# - https://esmithy.net/2015/05/05/rundebug-as-root-in-pycharm/
	# - https://intellij-support.jetbrains.com/hc/en-us/community/posts/206587695-How-to-run-debug-programs-with-super-user-privileges?page=1#community_comment_205675625
	@pytest.mark.skipif(getpass.getuser() != 'root', reason="User isn't root and can't create tmpfs")
	def test_mkdir_no_space(self):
		sourceDir: Path = self.filesystem.operations.disk.sourceDir
		try:
			os.system(f"mount -t tmpfs tmpfs {sourceDir} -o size={FS_SIZE}M")

			rand_path = lambda: os.path.join(self.mount_info.mountDir, rand_string())
			f1 = rand_path()
			f2 = rand_path()
			f3 = rand_path()

			# fill up space on drive and leave enough for making a few directories.
			# os.O_SYNC as we need to skip any caching (Trying to fill the disk right up)
			fp = os.open(f1, os.O_CREAT | os.O_WRONLY | os.O_SYNC, self.mode)
			DIR_SIZE_BYTES = 4096
			zero_fill = (FS_SIZE * 1024 * 1024 - 1024) * b"\x01"
			os.write(fp, zero_fill)
			os.close(fp)

			# should be ok
			os.mkdir(f2)
			with pytest.raises(FUSEError) as e:
				# should fail with errno.NOSPC
				for i in range(1, 10):
					os.mkdir(rand_path())
			# Apparently doesn't work whyever
			# figure it out on monday
		finally:
			os.system(f"umount {sourceDir}")
