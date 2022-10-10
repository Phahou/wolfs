#!/usr/bin/env python
# -*- coding: utf-8 -*-
# type: ignore
import pyfuse3
import pytest
from src.libwolfs.translator import InodeTranslator, PathTranslator, MountFSDirectoryInfo
from tempfile import TemporaryDirectory, NamedTemporaryFile
from pathlib import Path
from test.util import rand_string
from src.libwolfs.vfs import VFS
from pyfuse3 import EntryAttributes

class TestPathTranslator:
	src: TemporaryDirectory
	cache: TemporaryDirectory
	mount: TemporaryDirectory
	temp_f: NamedTemporaryFile
	translator: PathTranslator

	@classmethod
	def setup_class(cls) -> None:
		cls.src = TemporaryDirectory()
		cls.cache = TemporaryDirectory()
		cls.mount = TemporaryDirectory()

	def setup_method(self) -> None:
		mount_info = MountFSDirectoryInfo(self.src.name, self.cache.name, self.mount.name)
		self.translator = PathTranslator(mount_info)
		self.temp_f = NamedTemporaryFile(dir=self.src.name)

	def teardown_method(self) -> None:
		del self.translator
		del self.temp_f

################################################################################
	def test_toRoot_initDirs(self) -> None:
		assert "/" == self.translator.toRoot(self.mount.name)
		assert "/" == self.translator.toRoot(self.src.name)
		assert "/" == self.translator.toRoot(self.cache.name)

	def test_toRoot_subdir(self) -> None:
		fname = rand_string()
		froot = "/" + fname
		f_mnt = Path(self.mount.name + froot)
		f_src = Path(self.src.name + froot)
		f_tmp = Path(self.cache.name + froot)
		assert self.translator.toRoot(f_mnt)\
			== self.translator.toRoot(f_src)\
			== self.translator.toRoot(f_tmp)\
			== froot

	def test_translations_complete(self) -> None:
		fname = rand_string()
		froot = "/" + fname
		f_mnt = Path(self.mount.name + froot)
		f_src = Path(self.src.name   + froot)
		f_tmp = Path(self.cache.name + froot)
		assert self.translator.toMnt(f_src)\
			== self.translator.toMnt(f_tmp)\
			== f_mnt
		assert self.translator.toSrc(f_mnt)\
			== self.translator.toSrc(f_tmp)\
			== f_src

		assert self.translator.toTmp(f_mnt)\
			== self.translator.toTmp(f_src)\
			== f_tmp

	def test_translations_subdir(self) -> None:
		fname = rand_string()
		subdir = rand_string()
		root_expect = "/" + subdir + "/" + fname

		f_mnt = Path(self.mount.name + root_expect)
		f_src = Path(self.src.name   + root_expect)
		f_tmp = Path(self.cache.name + root_expect)
		assert self.translator.toMnt(f_src) == self.translator.toMnt(f_tmp) == f_mnt
		assert self.translator.toSrc(f_mnt) == self.translator.toSrc(f_tmp) == f_src
		assert self.translator.toTmp(f_mnt) == self.translator.toTmp(f_src) == f_tmp


class TestInodeTranslator:
	src: TemporaryDirectory
	cache: TemporaryDirectory
	temp_f: NamedTemporaryFile
	translator: InodeTranslator

	@classmethod
	def setup_class(cls) -> None:
		cls.src = TemporaryDirectory()
		cls.cache = TemporaryDirectory()

	def setup_method(self) -> None:
		mount_info = MountFSDirectoryInfo(self.src.name, self.cache.name, self.cache.name)
		self.translator = InodeTranslator(mount_info)
		self.temp_f = NamedTemporaryFile(dir=self.src.name)

	def teardown_method(self) -> None:
		del self.translator
		del self.temp_f

################################################################################
	def test_insertion_deletion_single_path(self) -> int:
		ino = self.translator.path_to_ino(self.temp_f.name)
		path_ino_map = self.translator._InodeTranslator__path_ino_map
		ino_path_map = self.translator._InodeTranslator__ino_path_map
		rpath: str = self.translator.toRoot(self.temp_f.name)
		assert ino_path_map[ino] == rpath
		assert path_ino_map[rpath] == ino

		del self.translator[(ino, self.temp_f.name)]

		# shouldn't hurt getting the dicts again explicitly
		path_ino_map = self.translator._InodeTranslator__path_ino_map
		ino_path_map = self.translator._InodeTranslator__ino_path_map
		assert ino_path_map.get(ino) is None
		assert path_ino_map.get(rpath) is None
		return ino

	def test_insertion_lookup_deletion_single_path(self) -> None:
		ino = self.translator.path_to_ino(self.temp_f.name)
		assert ino == self.translator.path_to_ino(self.temp_f.name)
		assert self.translator.toRoot(self.temp_f.name) == self.translator.ino_to_rpath(ino)

	def test_ino_generation(self) -> None:
		t = NamedTemporaryFile(dir=self.src.name)
		ino = self.translator.path_to_ino(t.name)
		ino2 = self.translator.path_to_ino(self.temp_f.name)
		assert ino != ino2
		assert ino < ino2

	def test_ino_reuse(self) -> None:
		# neutral insert __freed_inos should have one ino
		ino = self.test_insertion_deletion_single_path()
		freed_inos = self.translator._InodeTranslator__freed_inos
		assert len(self.translator._InodeTranslator__freed_inos) > 0
		assert ino in freed_inos

		# test resuse of ino
		t2 = NamedTemporaryFile(dir=self.src.name)
		assert self.translator.path_to_ino(t2.name, reuse_ino=ino) == ino

	def test_hardlink_insertion_two_paths(self) -> None:
		# boilerplate setup: insert 1st path
		trans = self.translator
		ino = trans.path_to_ino(self.temp_f.name)
		assert isinstance(trans._InodeTranslator__ino_path_map[ino], str)
		assert trans.ino_to_rpath(ino) == trans._InodeTranslator__ino_path_map[ino]

		# insert 2nd path via hardlink
		tmp_f2 = NamedTemporaryFile(dir=self.src.name)
		trans.add_hardlink(ino, tmp_f2.name)
		ino2 = trans.path_to_ino(tmp_f2.name)
		assert trans.path_to_ino(self.temp_f.name) == ino
		assert ino == ino2
		assert isinstance(trans._InodeTranslator__ino_path_map[ino], set)

		# check for consitency
		rpath = trans.ino_to_rpath(ino)
		assert ino == trans._InodeTranslator__path_ino_map[rpath]

	def test_hardlink_deletion_two_paths(self) -> None:
		# shortcuts
		trans = self.translator

		# fill up data structure
		self.test_hardlink_insertion_two_paths()

		# get inos & paths
		ino = trans.path_to_ino(self.temp_f.name)
		rpath_set: set = trans.ino_to_rpath(ino, need_set=True)
		rpath_1, rpath_2 = tuple(rpath_set)
		assert isinstance(rpath_set, set)
		assert isinstance(rpath_1, str) and isinstance(rpath_2, str)
		assert {rpath_1, rpath_2} == trans._InodeTranslator__ino_path_map[ino]

		# delete 1st path
		del trans[(ino, rpath_1)]

		# check rpath_1 is not anymore in internal mappings but rpath_2 is as is ino
		assert rpath_2 in trans._InodeTranslator__path_ino_map
		assert rpath_1 not in trans._InodeTranslator__path_ino_map
		assert ino in trans._InodeTranslator__ino_path_map

		# consistency check
		rpath_set_without_rpath_1 = trans.ino_to_rpath(ino, need_set=True)
		assert rpath_set_without_rpath_1 == rpath_2
		assert trans.path_to_ino(rpath_2) == ino
		assert trans.ino_to_rpath(ino) == rpath_2

		del trans[(ino, rpath_2)]
		# check ino and rpath_2 are not anymore in internal mappings
		assert rpath_2 not in trans._InodeTranslator__path_ino_map
		assert ino not in trans._InodeTranslator__ino_path_map
		assert ino in trans._InodeTranslator__freed_inos

	@pytest.mark.skip
	def test_softlink_insertion(self):
		pass

	@pytest.mark.skip
	def test_softlink_deletion(self):
		pass

	@pytest.mark.skip
	def test_hardlink_to_softlink(self):
		pass

	def test_execption_on_too_large_ino(self) -> None:
		ino = self.translator.path_to_ino(self.temp_f.name)
		t2 = NamedTemporaryFile(dir=self.src.name)
		t2.close()
		ino2 = ino + 2
		with pytest.raises(ValueError) as e:
			self.translator.path_to_ino(t2.name, reuse_ino=ino2)
			assert "Reused ino is larger than largest generated ino" in e.value

	def test_exception_on_already_used_ino(self) -> None:
		ino = self.translator.path_to_ino(self.temp_f.name)
		t2 = NamedTemporaryFile(dir=self.src.name)
		t2.close()
		ino2 = ino
		with pytest.raises(ValueError) as e:
			self.translator.path_to_ino(t2.name, reuse_ino=ino2)
			assert "is not in freed ino set" in e.value

	def test_negative_inos(self) -> None:
		ino = self.translator.path_to_ino(self.temp_f.name)
		t2 = NamedTemporaryFile(dir=self.src.name)
		t2.close()
		ino2 = -ino
		assert abs(ino) != ino2
		with pytest.raises(AssertionError) as e:
			self.translator.path_to_ino(t2.name, reuse_ino=ino2)
			assert "reuse_ino can't be negative" in e.value

################################################################################
# convinience functions (basically just shortcuts to commonly used operations)
	def test_ino_toTmp(self):
		tmp_f = self.temp_f.name
		trans = self.translator
		ino = trans.path_to_ino(tmp_f)
		assert trans.toRoot(tmp_f) == trans.ino_to_rpath(ino)
		assert trans.toTmp(tmp_f) == trans.ino_toTmp(ino)

	def test_ino_toTmp_hardlinks(self):
		trans = self.translator
		tmp_f = self.temp_f.name
		tmp_f2 = NamedTemporaryFile(dir=self.src.name)
		ino = trans.path_to_ino(tmp_f)
		trans.add_hardlink(ino, tmp_f2.name)
		assert trans.ino_toTmp(ino) in map(lambda x: trans.toTmp(x), trans.ino_to_rpath(ino, need_set=True))

	@pytest.mark.skip
	def test_ino_toTmp_softlinks(self):
		pass
