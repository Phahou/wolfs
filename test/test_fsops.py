#!/usr/bin/python

# mypy ignore whole file
# type: ignore

from pathlib import Path
SKIP = False

import pytest
import os

# ------------------------------------------------------------------
#@pytest.fixture
# result of sample() gets copied to tests with same formal parameter
# useful for injecting pre conditions like db connections etc

#@pytest.mark.parametrize
# multiple sets of input that get tested

#@pytest.mark.<some marker> can then be used in $ pytest -m <marker>
# @pytest.mark.skipif(not Path(MNT_PATH).is_mount() or SKIP, reason=f"{MNT_PATH} is not mounted or SKIP=True")
@pytest.mark.skip
class TestFSOperations:
    def test_copy_sameAttrsAsSrcDir(self):
        pass

    def test_copy_onlyCachesMostRecentFiles(self):
        pass

    def test_open_RedirectsToCacheFile(self):
        pass

    def test_read_redirectsToCache(self):
        pass

    def test_readdir_sameAsSrcDir(self):
        pass

    def test_listDir(self):
        pass

    def test_deepDir(self):
        pass

    def test_openFile(self):
        pass
