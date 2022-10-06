#!/usr/bin/env python
# -*- coding: utf-8 -*-
# type: ignore
import pytest
from src.libwolfs.translator import InodeTranslator, PathTranslator, MountFSDirectoryInfo
from tempfile import TemporaryDirectory, NamedTemporaryFile
from pathlib import Path
from test.util import rand_string

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

	@pytest.mark.skip
	def test_insertion_same_ino_different_paths(self) -> None:
		ino = self.translator.path_to_ino(self.temp_f.name)
		t2 = NamedTemporaryFile(dir=self.src.name)
		ino2 = self.translator.path_to_ino(t2.name, reuse_ino=ino)
		assert ino == ino2

	@pytest.mark.skip
	def test_deletion_same_ino_different_paths(self) -> None:
		# softlinks need to be enabled
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
