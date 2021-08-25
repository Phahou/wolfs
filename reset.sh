#!/usr/bin/bash

if mount | grep mnt/mountpoint > /dev/null; then
    umount mnt/mountpoint
fi

#rm -rf mnt/local_data
#mkdir mnt/local_data
# while debugging use ram
rm -rf /tmp/wolfs_data/*
mkdir -p /tmp/wolfs_data