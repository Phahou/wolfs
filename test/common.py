#!/usr/bin/env python
# type: ignore

import subprocess
from src.fsops.vfsops import VFSOps
from src.libwolfs.translator import MountFSDirectoryInfo
from tempfile import mkdtemp
from pathlib import Path
from time import sleep
import pyfuse3

TESTDIR_PREFIX = "/tmp/wolfs_test"
MICROSECOND = 1 / 1_000_000

def micro_sleep(timeout: int):
	sleep(timeout * MICROSECOND)

def mount_testfs(ops: VFSOps, mount_point: str | Path) -> None:
	fuse_options = set(pyfuse3.default_options)
	fuse_options.add('fsname=wolfs_pytest')
	pyfuse3.init(ops, mount_point, fuse_options)

def umount_testfs(mount_point: str | Path):
	mount_point = Path(mount_point)
	subprocess.call(f"fusermount -z -u {mount_point}")
	# ensure it is unmounted:
	assert not mount_point.is_mount()

def create_mount_info(testdir_prefix: str = TESTDIR_PREFIX) -> MountFSDirectoryInfo:
	p = Path(testdir_prefix)
	if not p.exists():
		p.mkdir()
	test_dir = p.absolute().__str__()
	src = mkdtemp(prefix='src_', dir=test_dir)
	cache = mkdtemp(prefix='cache_', dir=test_dir)
	mount = mkdtemp(prefix='mnt_', dir=test_dir)

	mount_info: MountFSDirectoryInfo = MountFSDirectoryInfo(src, cache, mount)
	return mount_info

