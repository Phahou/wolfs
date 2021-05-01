#!/usr/bin/python3
from wakeonlan import send_magic_packet
from icmplib import ping
import trio
from errors import MountError, WakeupError
from pathlib import Path

import ctypes
import ctypes.util
import os
# docs: https://ftp.gnu.org/old-gnu/Manuals/glibc-2.2.3/html_node/libc_629.html

libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
libc.mount.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_ulong, ctypes.c_char_p)
libc.umount2.argtypes = (ctypes.c_char_p, ctypes.c_int)


def mount(source, target, fs, options=''):
	ret = libc.mount(source.encode(), target.encode(), fs.encode(), 0, options.encode())
	if ret < 0:
		errno = ctypes.get_errno()
		raise MountError(errno,
						 f"Error mounting {source} ({fs}) on {target} with options '{options}': {os.strerror(errno)}")


def umount2(file, flags):
	ret = libc.umount2(file.encode(), flags.encode())
	if ret < 0:
		errno = ctypes.get_errno()
		raise MountError(errno, f"Error umounting {file}: {os.strerror(errno)}")


class RemoteNode:

	def __init__(self, source: str, mountpoint: str, remoteFS: str, mountOpts: str,
				 mac: str, ip: str, ping_timeout=3.0, wakeuptimeout=45.0):
		assert ping_timeout > wakeuptimeout, 'RemoteNode: (ping_timeout > wakeuptimeout)!'
		# mount specific
		self.mountPoint = Path(mountpoint)
		self.remoteSource = Path(source)
		self.remoteFS = remoteFS
		self.mountOpts = mountOpts

		# WoL specific
		self.wakeuptimeout = wakeuptimeout
		self.ping_timeout = ping_timeout
		self.mac = mac
		self.ip = ip

	def makeAvailable(self):
		# TODO: check if it needs to be unmounted or it could be simply remounted
		#       maybe use cffi as it could get the Option Flags in the mount functions
		#       (something like the MNT_FORCE flag in the mount function)

		# only mount the filesystem for now or pretend you do
		print('pretended to made remoteFS Availaible')

	def isOnline(self):
		"""sends every 0.5s a ping to check if host is up. Returns after first received ping."""
		host = ping(self.ip, interval=0.5, timeout=self.ping_timeout, privileged=False)
		return host.is_alive

	async def wakeup(self):
		"""sends every sec a magic packet to across network assuming it is in the same LAN"""
		with trio.move_on_after(self.wakeuptimeout) as cancel_scope:
			while 1:
				if self.isOnline():
					break
		if cancel_scope.cancelled_caught:
			raise WakeupError

	def isMounted(self):
		"""checks if remote FS is accessible"""
		return self.mountPoint.is_mount()

	async def mountRemoteFS(self):
		"""
		:NOTICE: Needs to have the user option in fstab enabled to work
		Mounts the remote filesystem
		"""
		if not self.isOnline():
			await self.wakeup()
			mount(self.remoteSource, self.mountPoint, fs=self.remoteFS, options=self.mountOpts)
