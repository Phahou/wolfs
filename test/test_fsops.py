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
@pytest.mark.skip
class TestFSOperations:
    def copy_sameAttrsAsSrcDir(self):
        pass

    def copy_onlyCachesMostRecentFiles(self):
        pass

    def open_RedirectsToCacheFile(self):
        pass

    def read_redirectsToCache(self):
        pass

    def readdir_sameAsSrcDir(self):
        pass

    def tst_listDir(self):
        adir = os.listdir(TEST_PATH)
        print(adir)

    def tst_deepDir(self):
        adir = os.listdir(TEST_PATH + '/Ordner')
        print(adir)

    def openFile(self):
        with open(os.path.join(TEST_PATH, 'read.txt'),'r') as f:
            f.read()


#def test_DirectoryAPI():
#    runTest(tst_listDir)
#    runTest(tst_deepDir)
#    runTest(openFile)
