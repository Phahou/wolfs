#!/usr/bin/env python
# type: ignore

import errno
import os
import time
from pathlib import Path

import pytest
from IPython import embed

embed = embed
import random
from src.libwolfs.disk import Disk
from test.util import name_generator, nano_sleep, pseudo_file
from src.libwolfs.translator import MountFSDirectoryInfo

with open(__file__, 'rb') as fh:
	TEST_DATA = fh.read()


###################################################
# Helpers
###################################################

CACHE_SIZE = 4 * 1024
USE_NOATIME = True
CACHE_THRESHOLD = 0.7

def prep_Disk(sourceDir, cacheDir, maxCacheSize=CACHE_SIZE, noatime=USE_NOATIME,
			  cacheThreshold=CACHE_THRESHOLD):
	mount_info = MountFSDirectoryInfo(sourceDir, cacheDir, cacheDir) # ignore mountpoint
	return Disk(mount_info, maxCacheSize=maxCacheSize, noatime=noatime,
				cacheThreshold=cacheThreshold)


def clean_Disk():
	pass

def get_src_cache_directory_pair(tmpdir_factory):
	tmpdir_source = tmpdir_factory.mktemp("src")
	tmpdir_cache = tmpdir_factory.mktemp("cache")
	return tmpdir_source, tmpdir_cache


###################################################
# Unit tests
###################################################
class TestDisk:
	def test_init_disk(self, tmpdir_factory):
		tmpdir_source, tmpdir_cache = get_src_cache_directory_pair(tmpdir_factory)

		# check if everything goes fine
		disk = prep_Disk(tmpdir_source, tmpdir_cache)
		assert disk.sourceDir == tmpdir_source
		assert disk.cacheDir == tmpdir_cache

		def nodir(faulty_disk_constructor) -> None:
			with pytest.raises(SystemExit) as pytest_wrapped_e:
				faulty_disk_constructor()
			assert pytest_wrapped_e.type == SystemExit
			assert pytest_wrapped_e.value.code == errno.ENOENT

		# src doesnt exist
		os.rmdir(tmpdir_source)
		nodir(lambda: prep_Disk(sourceDir=tmpdir_source, cacheDir=tmpdir_cache))
		tmpdir_source = tmpdir_factory.mktemp("src")

		# cache doesnt exist
		os.rmdir(tmpdir_cache)
		nodir(lambda: prep_Disk(sourceDir=tmpdir_source, cacheDir=tmpdir_cache))

		# both don't exist
		os.rmdir(tmpdir_source)
		nodir(lambda: prep_Disk(sourceDir=tmpdir_source, cacheDir=tmpdir_cache))

	def test_toPathFuncs(self, tmpdir_factory):
		tmpdir_source, tmpdir_cache = get_src_cache_directory_pair(tmpdir_factory)
		disk = prep_Disk(tmpdir_source, tmpdir_cache)

		# type str tests
		root_file = "test.py"
		# check if a root file is correctly converted
		assert disk.toSrc(root_file) == Path(f"{tmpdir_source}/{root_file}")
		assert disk.toTmp(root_file) == Path(f"{tmpdir_cache}/{root_file}")
		assert disk.toRoot(root_file) == f"/{root_file}"

		subdir = "dir/test.py"
		# check if a file in a subdirectory works
		assert disk.toSrc(subdir) == Path(f"{tmpdir_source}/{subdir}")
		assert disk.toTmp(subdir) == Path(f"{tmpdir_cache}/{subdir}")
		assert disk.toRoot(subdir) == f"/{subdir}"

		# check if conversion works
		root_file_p = Path(root_file)
		assert disk.toSrc(root_file_p) == disk.toSrc(root_file)
		assert disk.toTmp(root_file_p) == disk.toTmp(root_file)
		assert disk.toRoot(root_file_p) == disk.toRoot(root_file)

		subdir_p = Path(subdir)
		assert disk.toSrc(subdir_p) == disk.toSrc(subdir)
		assert disk.toTmp(subdir_p) == disk.toTmp(subdir)
		assert disk.toRoot(subdir_p) == disk.toRoot(subdir)

	def test_canStore(self, tmpdir_factory):
		tmpdir_source, tmpdir_cache = get_src_cache_directory_pair(tmpdir_factory)
		disk = prep_Disk(tmpdir_source, tmpdir_cache, maxCacheSize=1)
		DIRECOTRIES_PER_MEGABYTE = int(1024 // 4)  # 256

		# for normal operation: (directory parents are available)
		enough: Path = Path(os.path.join(tmpdir_source, name_generator()))
		for i in range(1, 5):
			pseudo_file(enough, i * DIRECOTRIES_PER_MEGABYTE)
			assert disk.canStore(
				Path(enough)), f"Couldn't store {enough} although there should be enough space  ({i * 25}%)!"
			os.remove(enough)

		not_enough: Path = Path(os.path.join(tmpdir_source, name_generator()))
		pseudo_file(not_enough, 5 * DIRECOTRIES_PER_MEGABYTE)
		assert not disk.canStore(not_enough), f"Could store {not_enough} although there shouldnt be enough space!"
		os.remove(not_enough)

		# for directories which have to be created
		previous_name: Path = Path(os.path.join(tmpdir_source, name_generator()))
		alloced_dirs = [previous_name]
		os.mkdir(previous_name)
		for i in range(1, 5):
			subdir_enough: Path = Path(os.path.join(previous_name.__str__() + '/', name_generator()))
			pseudo_file(subdir_enough, 4 * (DIRECOTRIES_PER_MEGABYTE - i))
			assert_message = f"Couldn't store {subdir_enough}, although there's space for directories in between!"
			assert disk.canStore(Path(subdir_enough)), assert_message
			os.remove(subdir_enough)
			previous_name = subdir_enough
			os.mkdir(previous_name)
			alloced_dirs.append(previous_name)

		for d in reversed(alloced_dirs):
			os.rmdir(d)

		subdir_not_enough_root = os.path.join(tmpdir_source, name_generator())
		os.mkdir(subdir_not_enough_root)
		subdir_not_enough: Path = Path(os.path.join(subdir_not_enough_root, name_generator()))
		pseudo_file(subdir_not_enough, 4 * DIRECOTRIES_PER_MEGABYTE)
		assert not disk.canStore(
			subdir_not_enough), f"Could store {not_enough}, although there shouldn't be enough space!"
		os.remove(subdir_not_enough)
		os.rmdir(subdir_not_enough_root)

	def test_copystat(self, tmpdir_factory):
		tmpdir_source, _ = get_src_cache_directory_pair(tmpdir_factory)
		# LATER: also copy xAttrs if there are any
		src_dir: Path = Path(os.path.join(tmpdir_source, name_generator()))
		os.mkdir(src_dir)

		src: Path = Path(os.path.join(src_dir, name_generator()))

		dst: Path = Path(os.path.join(src_dir, name_generator()))

		pseudo_file(src)
		nano_sleep()
		pseudo_file(dst)

		Disk.copystat(src, dst)
		stat_src = os.stat(src)
		stat_dst = os.stat(dst)
		for attr in ['st_mode', 'st_dev',
					 'st_uid', 'st_gid', 'st_size',
					 'st_atime', 'st_mtime']:
			assert getattr(stat_src, attr) == getattr(stat_dst, attr), f"Mismatch of copied attribute {attr}"
		# assert stat_src_dir.st_mode == stat_src.st_mode, f"Mode in {src_dir} wasn't copied from {src}"
		os.remove(dst)
		os.remove(src)
		os.rmdir(src_dir)

	def test_mkdir_p(self, tmpdir_factory):
		tmpdir_source, tmpdir_cache = get_src_cache_directory_pair(tmpdir_factory)

		disk = prep_Disk(tmpdir_source, tmpdir_cache, maxCacheSize=1)

		# generate a bunch of random nested folders
		src_dir: Path = Path(os.path.join(tmpdir_source, name_generator()))

		def create_a_bunch_of_subdirs(src_dir: Path, amount: int) -> Path:
			for i in range(amount):
				try:
					src_dir.mkdir()
				except FileExistsError:
					pass
				nano_sleep()
				src_dir: Path = Path(os.path.join(src_dir, name_generator()))
			nano_sleep()
			src_dir.mkdir()
			return src_dir

		# create root
		src_dir = create_a_bunch_of_subdirs(src_dir, 0)
		actual_size, added_folders = disk.mkdir_p(src_dir)
		assert len(added_folders) == 1
		expected_size = os.stat(src_dir).st_size
		assert actual_size == expected_size, f"Actual size and book-keeped size mismatch {actual_size} {expected_size}"

		# create 2 subdirs
		SUBFOLDERS = 2
		src_dir = create_a_bunch_of_subdirs(src_dir, SUBFOLDERS)
		# perform cpdir().
		actual_size, added_folders = disk.mkdir_p(src_dir)
		assert len(added_folders) == SUBFOLDERS

		# result should have the same size as original directory
		expected_size = 0
		for i in added_folders:
			expected_size += os.stat(i).st_size

		assert actual_size == expected_size, f"Actual size and book-keeped size mismatch {actual_size} {expected_size}"

		def check_attrs(s_stat, f_stat):
			for attr in ['st_mode', 'st_dev',
						 'st_uid', 'st_gid', 'st_size',
						 'st_atime', 'st_mtime']:
				assert getattr(s_stat, attr) == getattr(f_stat, attr), f"{attr} failed for {src_dir} {f}"

		# check if directory structure is the same and if st_modes are the same
		for f in reversed(added_folders):
			s_stat, f_stat = Path(src_dir).stat(), f.stat()
			check_attrs(s_stat, f_stat)
			src_dir = src_dir.parent

		# cleanup
		for d in reversed(added_folders):  # skip root dir
			d.rmdir()

	@pytest.mark.skip
	def test_getSize(self):
		# later when symbolic links are added
		pass

	def test_path2Ino(self, tmpdir_factory):
		tmpdir_source, tmpdir_cache = get_src_cache_directory_pair(tmpdir_factory)
		disk = prep_Disk(tmpdir_source, tmpdir_cache, maxCacheSize=1)
		path: Path = Path(os.path.join(tmpdir_source, name_generator()))
		rpath = disk.toRoot(path)

		# 1. case: path is unkown -> new ino
		ino = disk.path_to_ino(path)
		assert disk._InodeTranslator__path_ino_map.get(rpath) == ino, f"path didnt save ino in internal dict"

		# 2. case: path is known -> same ino (normal ops)
		assert disk.path_to_ino(path) == ino, f"If known the same ino should be returned"

		# 3. case: reusing an ino (in rename ops)
		del disk[(ino, path.__str__())]
		ino_rpath = disk._InodeTranslator__path_ino_map.get(rpath)
		assert ino_rpath is None, f"{disk._InodeTranslator__path_ino_map} should contain {path} as it was inserted"
		__freed_inos = disk._InodeTranslator__freed_inos
		assert ino in __freed_inos, f"{__freed_inos} should contain {ino} as it was deleted"

		disk.path_to_ino(path, reuse_ino=ino)
		assert disk._InodeTranslator__path_ino_map.get(rpath) == ino, f"Should have reused the same ino"

		# 4. case: inodes grow only larger
		path2: Path = Path(os.path.join(tmpdir_source, name_generator()))
		ino2 = disk.path_to_ino(path2)
		assert ino < ino2, f"The generator of inodes should only generate larger inos"

	# 5. case: foreign translation update of inos
	# skip for now (what did I mean by that ?)

	###################################################
	# Class tests
	###################################################

	def test__cp_path(self):
		pass

	def test_track(self):
		pass

	def test_untrack(self):
		pass

	def test_makeRoomForPath(self):
		pass

	def test_cp2Cache(self):
		pass

	def test_rebuildCacheDir(self):
		pass
