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
from test_config import REMOTE, MNT_DIR, DATA_DIR


TEST_FILE = __file__

with open(TEST_FILE, 'rb') as fh:
	TEST_DATA = fh.read()

#if __name__ == '__main__':
assert os.path.exists(REMOTE), 'Only use these tests if the remote file system is mounted!'
assert os.path.exists(MNT_DIR), 'Only use these tests if Wolfs is initialised'
#sys.exit(pytest.main([__file__] + sys.argv[1:]))

###################################################
# Helpers
###################################################

def name_generator():
	return 'testfile_%d' % (random.random() * 10000)

def prep_VFS():
	pass

def clean_VFS():
	pass

###################################################
# Unit tests
###################################################

def test_toPathFuncs():
	pass

def test_inode_to_cpath():
	pass

def test_del_inode():
	pass

def test_getInodeOf():
	pass

def test_addDirectory():
	pass

def test_add_path():
	pass

###################################################
# Class tests
###################################################

def test_addFilePath():
	pass
