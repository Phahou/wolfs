#!/usr/bin/python
from pathlib import Path
from queue import PriorityQueue

import sys
import traceback
from IPython import embed

embed = embed

from typing import Callable
import functools
from datetime import datetime
import inspect
from typing import Final

DEFAULT_CACHE_SIZE: Final[int] = 512
__ROOT_INODE__: Final[int] = 2

# this can be ignored fully as logging supports this too.....
# https://stackoverflow.com/questions/533048/how-to-log-source-file-name-and-line-number-in-python
class CallStackAware:
	def __str__(self) -> str:
		return __functionName__(self, 2)

def __functionName__(self: object, i: int = 1) -> str:
	return f"{Col.BY}{self.__class__.__name__}.{inspect.stack()[i][3]}(): {Col.END}"


def convert(func: Callable) -> Callable:
	@functools.wraps(func)
	def wrapper_type_cast(func, *args, **kwargs):  # type: ignore
		value = func(*args, **kwargs)
		print(type(value) + ': ' + value)
		# Do something after
		return value

	return wrapper_type_cast

def _exit(s: str) -> None:
	traceback.print_tb(None)
	sys.exit(s)


def datef(timestamp: int) -> str:
	return datetime.fromtimestamp(timestamp).strftime("%d.%b.%Y %H:%M")

def sizeof(obj: object) -> int:
	size: int = sys.getsizeof(obj)
	if isinstance(obj, dict):
		return size + sum(map(sizeof, obj.keys())) + sum(map(sizeof, obj.values()))
	elif isinstance(obj, (list, tuple, set, frozenset)):
		return size + sum(map(sizeof, obj))
	return size


def is_type(type_class: type, variable_list: list) -> bool:
	return all([isinstance(x, type_class) for x in variable_list])


def mute_unused(*args, **kwargs): # type: ignore
	return args, kwargs


def formatByteSize(b: float) -> str:
	j: int = 0
	sizes: list[str] = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
	while True:
		if b / 1024 > 1.0:
			b = b / 1024
			j += 1
		else:
			return f'{float(b):.2} {sizes[j]}'

class MaxPrioQueue(PriorityQueue):
	"""
	A Max Heap Queue:
	Shouldnt be used if negative and positive indeces are mixed
	"""

	# As I dont want to fiddle around with inverting items
	# while I have other problems at hand
	def push_nowait(self, item: tuple) -> None:
		"""Same as PriorityQueue.put_nowait()"""
		return self.put_nowait((-item[0], item[1]))

	def pop_nowait(self) -> tuple:
		"""Same as PriorityQueue.get_nowait()"""
		index, data = self.get_nowait()
		return -index, data


class Col:
	"""A simple Coloring class"""

	def __init__(self, obj: object):
		self.obj = obj

	def __str__(self):
		if isinstance(self.obj, int):
			return Col.inode(self.obj)
		elif isinstance(self.obj, Path):
			return Col.path(self.obj)
		elif isinstance(self.obj, str):
			return Col.file(self.obj)
		elif isinstance(self.obj, list):
			return Col.directory(self.obj)

	BOLD = '\033[1m'
	B = BOLD

	WHITE = '\033[37m'
	W = WHITE
	BW = BOLD + WHITE

	PURPLE = '\033[95m'
	BP = BOLD + PURPLE

	CYAN = '\033[96m'
	BC = BOLD + CYAN

	DARKCYAN = '\033[36m'
	BDC = BOLD + DARKCYAN

	BLUE = '\033[94m'
	BB = BOLD + BLUE

	GREEN = '\033[92m'
	BG = BOLD + GREEN

	YELLOW = '\033[93m'
	BY = BOLD + YELLOW

	RED = '\033[91m'
	BR = BOLD + RED

	UNDERLINE = '\033[4m'
	END = '\033[0m'

	@staticmethod
	def inode(o: object) -> str:
		"""Green"""
		return f'{Col.BG}{o}{Col.BW}'

	@staticmethod
	def path(o: object) -> str:
		"""Yellow"""
		return f'{Col.BY}{o}{Col.BW}'

	@staticmethod
	def file(o: object) -> str:
		"""Cyan"""
		return f'{Col.BC}{o}{Col.BW}'

	@staticmethod
	def directory(o: object) -> str:
		"""Purple"""
		return f'{Col.BP}{o}{Col.BW}'
