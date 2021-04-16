#!/usr/bin/bash

if mount | grep mnt/mountpoint > /dev/null; then
    umount mnt/mountpoint
fi

rm -rf mnt/local_data
mkdir mnt/local_data