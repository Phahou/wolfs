#!/usr/bin/python

# mypy ignore whole file
# type: ignore

from IPython import embed

TEST_FILE = __file__
TEST_PATH = './mnt/mountpoint'
import pytest
import time
import os

def runTest(test):
    time.sleep(1.0)
    test()
    print('------------------------------------------------------------------')

# ------------------------------------------------------------------
#@pytest.fixture
# result of sample() gets copied to tests with same formal parameter
# useful for injecting pre conditions like db connections etc

#@pytest.mark.parametrize
# multiple sets of input that get tested

#@pytest.mark.<some marker> can then be used in $ pytest -m <marker>
def copy_sameAttrsAsSrcDir():
    pass

def copy_onlyCachesMostRecentFiles():
    pass


def open_RedirectsToCacheFile():
    pass

def read_redirectsToCache():
    pass

def readdir_sameAsSrcDir():
    pass

def tst_listDir():
    adir = os.listdir(TEST_PATH)
    print(adir)

def tst_deepDir():
    adir = os.listdir(TEST_PATH + '/Ordner')
    print(adir)

def openFile():
    with open(os.path.join(TEST_PATH, 'read.txt'),'r') as f:
        f.read()


#def test_DirectoryAPI():
#    runTest(tst_listDir)
#    runTest(tst_deepDir)
#    runTest(openFile)
