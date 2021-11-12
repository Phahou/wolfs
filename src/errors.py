#!/usr/bin/env python


# Error strings
SOFTLINK_DISABLED_ERROR = "Softlinks are currently not implemented"
HARDLINK_DIR_ILLEGAL_ERROR = "Hardlinks to directories are illegal!"

class WolfsError(Exception):
	"""Base Exception class for HSM-CacheFS"""


class MountError(WolfsError):
	"""File System couldnt be mounted"""


class WakeupError(WolfsError):
	"""Remote Node couldnt be woken up"""


class NotEnoughSpaceError(WolfsError):
	"""Cache Directory has run out of storage space"""
