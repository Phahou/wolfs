#!/usr/bin/env python
# type: ignore
import random
import time
import os
import string
from pathlib import Path
MAX_PATH_SIZE: int = 4096

def coinflip() -> bool:
	return True if random.choice([0, 1]) == 0 else False

def rand_string(str_size: int = 16) -> str:
	result = ''.join([random.choice(string.ascii_letters) for c in range(str_size)])
	assert len(result) == str_size
	assert len(result) < MAX_PATH_SIZE
	return result


def name_generator():
	return 'testfile_%d' % (random.random() * 10000)


def nano_sleep():
	time.sleep(random.randrange(1000) / 100_000)

def pseudo_file(test_file: str | Path, wanted_filesize: int = None) -> None:
	if isinstance(test_file, Path):
		test_file = test_file.__str__()
	if wanted_filesize is None:
		wanted_filesize = os.stat(__file__).st_size
	# create pseudo files of a particular file size:
	# url: https://stackoverflow.com/questions/8816059/create-file-of-particular-size-in-python
	with open(test_file, "wb") as f:
		f.seek((wanted_filesize * 1024) - 1)
		f.write(b"\0")
	assert os.stat(test_file).st_size == 1024 * wanted_filesize

def rmtree(f: Path | str) -> None:
	if isinstance(f, str):
		f = Path(f)
	if f.is_file():
		f.unlink()
	else:
		for child in f.iterdir():
			rmtree(child)
		f.rmdir()
