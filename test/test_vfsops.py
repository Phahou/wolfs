#!/usr/bin/env python
# type: ignore

import os
import random

import pytest
import sys
import filecmp
import shutil
import errno
from IPython import embed
embed = embed
import random
import time
from test.test_config import REMOTE, MNT_DIR, DATA_DIR


TEST_FILE = __file__

with open(TEST_FILE, 'rb') as fh:
	TEST_DATA = fh.read()

#if __name__ == '__main__':
assert os.path.exists(REMOTE), 'Only use these tests if the remote file system is mounted!'
assert os.path.exists(MNT_DIR), 'Only use these tests if Wolfs is initialised'
#sys.exit(pytest.main([__file__] + sys.argv[1:]))

def name_generator():
	return 'testfile_%d' % (random.random() * 10000)

def checked_unlink(filename, path, isdir=False):
	fullname = os.path.join(path, filename)
	if isdir:
		os.rmdir(fullname)
	else:
		os.unlink(fullname)
	with pytest.raises(OSError) as exc_info:
		os.stat(fullname)
	assert exc_info.value.errno == errno.ENOENT
	fileList: list = os.listdir(path)
	assert filename not in fileList

def prep_write():
	name = os.path.join(MNT_DIR, name_generator())
	shutil.copyfile(TEST_FILE, name)
	os.statvfs(MNT_DIR)  # force flush
	return name

def prep_mkdir():
	name = os.path.join(MNT_DIR, name_generator())
	os.mkdir(name)
	#os.statvfs(MNT_DIR) # force push
	return name


# there might be a problem with a race condition but it is so unlikely to happen that we ignore it for now
# it can be triggered by opening an opened file directly after calling unlink
#   Fortunately for us it wont be a problem unless we directly reuse the inode number which we currently dont
#   (inodes numbers are just growing and freed inodes arent reused )
def test_unsynced_write():
	"""
	Fundamental I/O test
	Contains: create, write, unlink
	"""
	name = prep_write()
	assert filecmp.cmp(name, TEST_FILE, False)
	assert filecmp.cmp(name.replace(REMOTE, DATA_DIR), TEST_FILE)
	checked_unlink(name, MNT_DIR)

def test_write_fail():
	pass

def test_flushed_write():
	name = prep_write()
	assert filecmp.cmp(name, TEST_FILE, False)
	assert filecmp.cmp(name.replace(REMOTE, DATA_DIR), TEST_FILE)
	os.statvfs(MNT_DIR)
	assert filecmp.cmp(name.replace(REMOTE, MNT_DIR), TEST_FILE)
	checked_unlink(name, MNT_DIR)


def test_write_ops():
	pass

# read test skipped as they are basically only a passthrough


#def test_rename_raise_ENOTDIR():
#	"""
#	Test for renameing a file into a non existent dir.
#	Contains create, write, rename, unlink
#	"""
#	name = prep_write()
#	#dirs = [os.path.join(MNT_DIR, d) for d in os.listdir(MNT_DIR) if os.path.isdir(os.path.join(MNT_DIR, d))]
#	#dest = dirs[int(random.random() * len(dirs))]
#	dest = os.path.join(MNT_DIR, 'test/')
#	with pytest.raises(NotADirectoryError) as exc_info:
#		os.rename(name, dest)
#
#	assert exc_info.value.errno == errno.ENOTDIR, f'As {dest} is a dir and {name} is not this should have failed'
#	#checked_unlink(name, dest)
#
#def test_rename_File2File_same_directory():
#
#	name = prep_write()
#	dest = os.path.join(MNT_DIR, name_generator())
#	os.rename(name, dest)
#	assert os.path.exists(dest), f'As we simply renamed {name} -> {dest}'

