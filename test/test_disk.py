#!/usr/bin/env python
# type: ignore

import os
import random
import time
from pathlib import Path

import pytest
import sys
import filecmp
import shutil
import errno
from IPython import embed

embed = embed
import random
from test.test_config import REMOTE, MNT_DIR, DATA_DIR
from src.disk import Disk

TEST_FILE = __file__
tmp_dir = __file__+ "-tmp"
os.mkdir()
EMPTY_DIRECTORY_SIZE = os.stat(tmp_dir).st_size
os.rmdir(tmp_dir)
del tmp_dir

with open(TEST_FILE, 'rb') as fh:
	TEST_DATA = fh.read()

assert os.path.exists(REMOTE), 'Only use these tests if the remote file system is mounted!'
assert os.path.exists(MNT_DIR), 'Only use these tests if Wolfs is initialised'

###################################################
# Helpers
###################################################

CACHE_SIZE = 4 * 1024
USE_NOATIME = True
CACHE_THRESHOLD = 0.7

def name_generator():
	return 'testfile_%d' % (random.random() * 10000)

def nano_sleep():
	time.sleep(random.randrange(1000) / 100_000)

def prep_Disk(sourceDir=REMOTE, cacheDir=DATA_DIR, maxCacheSize=CACHE_SIZE, noatime=USE_NOATIME,
			  cacheThreshold=CACHE_THRESHOLD):
	return Disk(sourceDir=sourceDir, cacheDir=cacheDir, maxCacheSize=maxCacheSize, noatime=noatime,
				cacheThreshold=cacheThreshold)


def clean_Disk():
	pass

def pseudo_file(test_file, wanted_filesize):
	# create pseudo files of a particular file size:
	# url: https://stackoverflow.com/questions/8816059/create-file-of-particular-size-in-python
	with open(test_file, "wb") as f:
		f.seek((wanted_filesize * 1024) - 1)
		f.write(b"\0")
	assert os.stat(test_file).st_size == 1024*wanted_filesize

def make_test_file(path):
	with open(path, "w") as f:
		f.write(str(TEST_DATA, 'utf8'))

###################################################
# Unit tests
###################################################

def test_toPathFuncs():
	disk = prep_Disk()

	# type str tests
	root_file = "test.py"
	# check if a root file is correctly converted
	assert disk.toSrcPath(root_file) == Path(f"{REMOTE}/{root_file}")
	assert disk.toCachePath(root_file) == Path(f"{DATA_DIR}/{root_file}")
	assert disk.toRootPath(root_file) == f"/{root_file}"

	subdir = "dir/test.py"
	# check if a file in a subdirectory works
	assert disk.toSrcPath(subdir) == Path(f"{REMOTE}/{subdir}")
	assert disk.toCachePath(subdir) == Path(f"{DATA_DIR}/{subdir}")
	assert disk.toRootPath(subdir) == f"/{subdir}"

	# check if conversion works
	root_file_p = Path(root_file)
	assert disk.toSrcPath(root_file_p) == disk.toSrcPath(root_file)
	assert disk.toCachePath(root_file_p) == disk.toCachePath(root_file)
	assert disk.toRootPath(root_file_p) == disk.toRootPath(root_file)

	subdir_p = Path(subdir)
	assert disk.toSrcPath(subdir_p) == disk.toSrcPath(subdir)
	assert disk.toCachePath(subdir_p) == disk.toCachePath(subdir)
	assert disk.toRootPath(subdir_p) == disk.toRootPath(subdir)

def test_canStore():
	disk = prep_Disk(maxCacheSize=1)
	DIRECOTRIES_PER_MEGABYTE = int(1024 // 4) # 256

	# for for normal operation: (directory parents are available)
	enough: Path = Path(os.path.join(REMOTE, name_generator()))
	for i in range(1, 5):
		pseudo_file(enough, i*DIRECOTRIES_PER_MEGABYTE)
		assert disk.canStore(Path(enough)), f"Couldn't store {enough} although there should be enough space  ({i*25}%)!"
		os.remove(enough)

	not_enough: Path = Path(os.path.join(REMOTE, name_generator()))
	pseudo_file(not_enough, 5*DIRECOTRIES_PER_MEGABYTE)
	assert not disk.canStore(not_enough), f"Could store {not_enough} although there shouldnt be enough space!"
	os.remove(not_enough)

	# for directories which have to be created
	previous_name: Path = Path(os.path.join(REMOTE, name_generator()))
	alloced_dirs = [previous_name]
	os.mkdir(previous_name)
	for i in range(1, 5):
		subdir_enough: Path = Path(os.path.join(previous_name.__str__() + '/', name_generator()))
		pseudo_file(subdir_enough, 4*(DIRECOTRIES_PER_MEGABYTE-i))
		assert disk.canStore(Path(subdir_enough)), f"Couldn't store {subdir_enough}, although there's enough space even for the needed directories in between!"
		os.remove(subdir_enough)
		previous_name = subdir_enough
		os.mkdir(previous_name)
		alloced_dirs.append(previous_name)

	for d in reversed(alloced_dirs):
		os.rmdir(d)

	subdir_not_enough_root = os.path.join(REMOTE, name_generator())
	os.mkdir(subdir_not_enough_root)
	subdir_not_enough: Path = Path(os.path.join(subdir_not_enough_root, name_generator()))
	pseudo_file(subdir_not_enough, 4*DIRECOTRIES_PER_MEGABYTE)
	assert not disk.canStore(subdir_not_enough), f"Could store {not_enough} although there shouldn't be enough space!"
	os.remove(subdir_not_enough)
	os.rmdir(subdir_not_enough_root)

def test_copystat():
	# LATER: also copy xAttrs if there are any
	src_dir: Path = Path(os.path.join(REMOTE, name_generator()))
	os.mkdir(src_dir)

	src: Path = Path(os.path.join(src_dir, name_generator()))

	dst: Path = Path(os.path.join(src_dir, name_generator()))

	make_test_file(src)
	nano_sleep()
	make_test_file(dst)

	Disk.copystat(src, dst)
	stat_src = os.stat(src)
	stat_dst = os.stat(dst)
	for attr in ['st_mode', 'st_dev',
				 'st_uid', 'st_gid', 'st_size',
				 'st_atime', 'st_mtime']:
		assert getattr(stat_src, attr) == getattr(stat_dst, attr), f"Mismatch of copied attribute {attr}"
	#assert stat_src_dir.st_mode == stat_src.st_mode, f"Mode in {src_dir} wasn't copied from {src}"
	os.remove(dst)
	os.remove(src)
	os.rmdir(src_dir)

def test_mkdir_p():
	disk = prep_Disk(maxCacheSize=1)

	# generate a bunch of random nested folders
	src_dir: Path = Path(os.path.join(REMOTE, name_generator()))

	SUBFOLDERS = 2
	for i in range(SUBFOLDERS-1):
		src_dir.mkdir()
		nano_sleep()
		src_dir: Path = Path(os.path.join(src_dir, name_generator()))
	nano_sleep()
	src_dir.mkdir()

	# perform cpdir()
	actual_size, added_folders = disk.mkdir_p(src_dir)
	assert len(added_folders) == SUBFOLDERS

	# result should have the same size as original directory
	expected_size = SUBFOLDERS * EMPTY_DIRECTORY_SIZE
	assert actual_size == expected_size, f"Actual size and book-keeped size mismatch {actual_size} {expected_size}"

	def check_attrs(s_stat, f_stat):
		for attr in ['st_mode', 'st_dev',
				 'st_uid', 'st_gid', 'st_size',
				 'st_atime', 'st_mtime']:
			assert getattr(s_stat, attr) == getattr(f_stat, attr), f"{attr} failed for {src_dir} {f}"

	# check if directory structure is the same and if st_modes are the same
	for f in reversed(added_folders):
		s_stat, f_stat = Path(src_dir).stat(), f.stat()
		check_attrs(s_stat,f_stat)
		src_dir = src_dir.parent


def test_getSize():
	# later when symbolic links are added
	pass

def test_path2Ino():
	disk = prep_Disk(maxCacheSize=1)
	path: Path = Path(os.path.join(REMOTE, name_generator()))
	rpath = disk.toRootPath(path)

	# 1. case: path is unkown -> new ino
	ino = disk.path_to_ino(path)
	assert disk.path_ino_map.get(rpath) == ino, f"path didnt save ino in internal dict"

	# 2. case: path is known -> same ino (normal ops)
	assert disk.path_to_ino(path) == ino, f"If known the same ino should be returned"

	# 3. case: reusing an ino (in rename ops)
	disk.del_inode(ino, path.__str__())
	assert disk.path_ino_map.get(rpath) is None, f"{disk.path_ino_map} should contain {path} as it was inserted"
	__freed_inos = getattr(disk, '_' + disk.__class__.__name__ + '__freed_inos')
	assert ino in __freed_inos, f"{__freed_inos} should contain {ino} as it was deleted"

	disk.path_to_ino(path, reuse_ino=ino)
	assert disk.path_ino_map.get(rpath) == ino, f"Should have reused the same ino"

	# 4. case: inodes grow only larger
	path2: Path = Path(os.path.join(REMOTE, name_generator()))
	ino2 = disk.path_to_ino(path2)
	assert ino < ino2, f"The generator of inodes should only generate larger inos"

	# 5. case: foreign translation update of inos
	# skip for now (what did I mean by that ?)

###################################################
# Class tests
###################################################

def test__cp_path():
	disk = prep_Disk()
	pass

def test_track():
	pass

def test_untrack():
	pass

def test_makeRoomForPath():
	pass

def test_cp2Cache():
	pass

def test_rebuildCacheDir():
	pass
