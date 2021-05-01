#!/usr/bin/python
from queue import PriorityQueue

import sys
import traceback
# suppress 'unused' warnings
from IPython import embed

embed = embed

from datetime import datetime

DEFAULT_CACHE_SIZE = 512


def _exit(s: str):
	traceback.print_tb()
	sys.exit(s)


def datef(timestamp):
	return datetime.fromtimestamp(timestamp).strftime("%d.%b.%Y %H:%M")

def sizeof(obj):
	size = sys.getsizeof(obj)
	if isinstance(obj, dict): return size + sum(map(sizeof, obj.keys())) + sum(map(sizeof, obj.values()))
	if isinstance(obj, (list, tuple, set, frozenset)): return size + sum(map(sizeof, obj))
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
			return f'{float(b):.4} {sizes[j]}'

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

	@staticmethod
	def b(s):
		return f'{Col.B}{s}{Col.END}'

	WHITE = '\033[37m'
	W = WHITE
	BW = BOLD + WHITE

	@staticmethod
	def bw(s):
		return f'{Col.BW}{s}{Col.END}'

	PURPLE = '\033[95m'
	CYAN = '\033[96m'
	BC = BOLD + CYAN
	DARKCYAN = '\033[36m'

	BLUE = '\033[94m'
	BB = BOLD + BLUE

	GREEN = '\033[92m'
	BG = BOLD + GREEN

	@staticmethod
	def bg(s):
		return f'{Col.BG}{s}{Col.END}'

	YELLOW = '\033[93m'
	BY = BOLD + YELLOW

	@staticmethod
	def by(s):
		return f'{Col.BY}{s}{Col.END}'

	RED = '\033[91m'
	BR = BOLD + RED

	@staticmethod
	def br(s):
		return f'{Col.BR}{s}{Col.END}'

	UNDERLINE = '\033[4m'
	END = '\033[0m'
