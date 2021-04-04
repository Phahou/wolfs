#!/usr/bin/python
import shutil
import os
from IPython import embed
TEST_FILE = __file__
TEST_PATH = './mnt/mountpoint'
import time

def runTest(test):
    time.sleep(1.0)
    test()
    print('------------------------------------------------------------------')


def tst_listDir():
    adir = os.listdir(TEST_PATH)
    print(adir)

def tst_deepDir():
    adir = os.listdir(TEST_PATH + '/Ordner')
    print(adir)

def openFile():
    with open(os.path.join(TEST_PATH, 'read.txt'),'r') as f:
        f.read()


def cleanup():
    pass


def test_DirectoryAPI():
    runTest(tst_listDir)
    # runTest(tst_deepDir)
    runTest(openFile)


if __name__ == '__main__':
    test_DirectoryAPI()
