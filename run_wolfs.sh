#!/bin/bash

BACKEND="/tmp/wolfs_cache"
CACHE="/tmp/wolfs_backend"
MOUNTPOINT="/tmp/wolfs"

# unmount if still mounted
if mount | grep "$MOUNTPOINT" > /dev/null; then
    umount "$MOUNTPOINT"
fi

# cleanup / delete directories
if [[ -d "$BACKEND" ]]; then
  rm -rf "$BACKEND";
fi

if [[ -d "$CACHE" ]]; then
  rm -rf "$CACHE";
fi

if [[ -d "$MOUNTPOINT" ]]; then
  rm -rf "$MOUNTPOINT";
fi

mkdir -p "$BACKEND"     # backend storage
mkdir -p "$CACHE"       # cache storage
mkdir -p "$MOUNTPOINT"  # mountpoint

# start wolfs with 512 MB storage in the cache
python3 wolfs.py "$BACKEND" "$MOUNTPOINT" "$CACHE" --size 512 --debug
