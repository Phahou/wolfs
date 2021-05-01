#!/usr/bin/python

class HSMCacheError(Exception):
	"""Base Exception class for HSM-CacheFS"""


class MountError(HSMCacheError):
	"""File System couldnt be mounted"""


class WakeupError(HSMCacheError):
	"""Remote Node couldnt be woken up"""


class NotEnoughSpaceError(HSMCacheError):
	"""Cache Directory has run out of storage space"""
