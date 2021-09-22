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

def prep_Disk():
	pass

def clean_Disk():
	pass

###################################################
# Unit tests
###################################################

def test_toPathFuncs():
	pass

def test_canStore():
	pass

def test_cpAttrs():
	pass

def test_cpdir():
	pass

def test_getSize():
	pass

def test_path2Ino():
	pass

###################################################
# Class tests
###################################################

def test__cp2Dir():
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
