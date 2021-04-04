#!/usr/bin/python

class HSMCacheError(Exception):
	"""Base Exception class for HSM-CacheFS"""


class MountError(HSMCacheError):
	"""File System couldnt be mounted"""

class WakeupError(HSMCacheError):
	"""Remote Node couldnt be woken up"""