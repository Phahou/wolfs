#!/usr/bin/python
from queue import PriorityQueue

import sys
import traceback
# suppress 'unused' warnings
from IPython import embed

embed = embed

from datetime import datetime
import inspect

DEFAULT_CACHE_SIZE = 512
__ROOT_INODE__ = 2

def __functionName__(self, i=1):
	return f"{Col.BY}{self.__class__.__name__}.{inspect.stack()[i][3]}():{Col.BW}"


def _exit(s: str):
	traceback.print_tb()
	sys.exit(s)


def datef(timestamp):
	return datetime.fromtimestamp(timestamp).strftime("%d.%b.%Y %H:%M")

def sizeof(obj):
	size = sys.getsizeof(obj)
	if isinstance(obj, dict):
		return size + sum(map(sizeof, obj.keys())) + sum(map(sizeof, obj.values()))
	elif isinstance(obj, (list, tuple, set, frozenset)):
		return size + sum(map(sizeof, obj))
	return size


def is_type(type_class, variable_list):
	return all([isinstance(x, type_class) for x in variable_list])


def mute_unused(*args, **kwargs):
	return args, kwargs


def formatByteSize(b):
	j, sizes = 0, ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
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
	def push_nowait(self, item):
		"""Same as PriorityQueue.put_nowait()"""
		return self.put_nowait((-item[0], item[1]))

	def pop_nowait(self):
		"""Same as PriorityQueue.get_nowait()"""
		index, data = self.get_nowait()
		return -index, data


class Col:
	"""A simple Coloring class"""
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
	def inode(i):
		"""Green"""
		return f'{Col.BG}{i}{Col.BW}'

	@staticmethod
	def path(p):
		"""Yellow"""
		return f'{Col.BY}{p}{Col.BW}'

	@staticmethod
	def file(f):
		"""Cyan"""
		return f'{Col.BC}{f}{Col.BW}'
