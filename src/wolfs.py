#!/usr/bin/env python

# mypy ignore whole file
# type: ignore

from fsops import Wolfs as Operations
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import pyfuse3
import trio
import sys
from remote import RemoteNode
from vfsops import VFSOps
from util import Col

DEBUG = False
DEBUG_FUSE = False

import datetime
import logging
import time

old_factory = logging.getLogRecordFactory()
def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    switch = {
        'DEBUG': 'DEBUG',
        'INFO': 'INFO',
        'CRITICAL': 'CRIT',
        'ERROR': 'ERROR',
        'WARNING': 'WARN',
    }
    record.logLvl = switch[record.levelname]
    return record


class RuntimeFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()

    def formatTime(self, record, datefmt=None):
        duration = datetime.datetime.utcfromtimestamp(record.created - self.start_time)
        elapsed = duration.strftime('%M:%S')
        return "{}".format(elapsed)


def init_logging(debug=False):
    logging.setLogRecordFactory(record_factory)
    formatter = RuntimeFormatter(
        f'%(asctime)s.%(msecs)03d '
        '[%(logLvl)5s]'
        #[%(levelname)1s] '    
        #'%(threadName)'s'
        f'[%(name)8s.%(funcName)-10s:%(lineno)3s] | %(message)s {Col.END}',
        # datefmt="%Y-%m-%d %H:%M:%S"
        datefmt="%H:%M:%S"
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


def parse_args(args):
    """Parse command line"""
    example_usuage = (
        "Example Usage:\n\n"
        "  wolfs $HOME/.local/lib mnt/mountpoint /tmp/wolfs_data/ --size 8096 --debug\n"
    )

    parser = ArgumentParser(epilog=example_usuage, formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument('source', type=str,
                        help='Directory tree to mirror')
    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('cache', type=str,
                        help='Local Datastore of remote Directory')
    parser.add_argument('--metadb', type=str, default='metaInfo.db',
                        help='sqlite3-directory storage')
    parser.add_argument('--log', type=str, default='fileJournal.log',
                        help='Journal-file to write logs to')
    parser.add_argument('--debug', action='store_true', default=DEBUG,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=DEBUG_FUSE,
                        help='Enable FUSE debugging output')
    parser.add_argument('--size', type=int, default=VFSOps._DEFAULT_CACHE_SIZE,
                        help='Size of the Cache in Megabytes')
    return parser.parse_args(args)


def main():
    options = parse_args(sys.argv[1:])
    init_logging(options.debug)
    remote = RemoteNode(options.source, options.mountpoint, 'ext4', None, None, None)
    operations = Operations(remote, options.source, options.cache, metadb=options.metadb, logFile=options.log,
                            maxCacheSizeMB=options.size)

    # log.debug('Mounting...')
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=wolfs')
    if options.debug_fuse:
        fuse_options.add('debug')
    pyfuse3.init(operations, options.mountpoint, fuse_options)

    unmounted = False
    try:
        # log.debug('Entering main loop..')
        trio.run(pyfuse3.main)
    except KeyboardInterrupt:
        # log.debug('Unmounting due to Ctrl+C')
        pyfuse3.close()
        unmounted = True
    except:
        pyfuse3.close(unmount=False)
        raise

    if unmounted:
        return
    # log.debug('Unmounting..')
    pyfuse3.close()


if __name__ == '__main__':
    main()
