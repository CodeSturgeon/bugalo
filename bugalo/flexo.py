#!/usr/bin/env python

import os
import re
import sys
import logging
import datetime

from optparse import OptionParser
from bugalo import *

from pprint import pprint as pp

MB = (1024*1024)
log = logging.getLogger()

def main():
    usage = "Usage: %prog [options] path [path, ...] "
    parser = OptionParser(usage=usage)
    parser.add_option("-b", "--batch-mode", dest="batch", action="store_true",
                        default=False, help="Use batch mode on folders")
    parser.add_option("-z", "--zip-path", dest="zip_path", help="zip output path",
                        default = '.')
    parser.add_option("-c", "--chunk-size", dest="size", type="int", default=500,
                        help="chunk size in MiB")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                        default=False, help="turn on info messages")
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
                        default=False, help="turn on debugging")
    parser.add_option("-n", "--no-zip", dest="nozip", action="store_true",
                        default=False, help="dry run")
    (opt, args) = parser.parse_args()

    # Setup logging
    log.setLevel(logging.WARN)
    log.addHandler(logging.StreamHandler())
    if opt.verbose: log.setLevel(logging.INFO)
    if opt.debug: log.setLevel(logging.DEBUG)

    if len(args) == 0:
        parser.print_usage()
        sys.exit(1)

    if opt.batch:
        now_date = datetime.datetime.now().strftime('%Y%m%d')
        batch_re = re.compile('(?P<name>.*?)_(?P<date>.*)')
        seq = 0
        for batch_path in args:
            source_folders = os.listdir(batch_path)
            for src in source_folders:
                # Ignore dot files
                if src.startswith('.'): continue
                abs_src = os.path.join(batch_path,src)
                if os.path.isfile(abs_src):
                    log.warn('%s is not a folder - skipping'%abs_src)
                    continue
                try:
                    vendor = batch_re.match(src).groups()[0]
                except AttributeError:
                    log.warn('%s is not named correctly - skipping'%abs_src)
                    continue
                prefix = '%s_%s'%(vendor, now_date)
                seq = find_and_zip(abs_src, opt.size, opt.zip_path,
                                           prefix, opt.nozip, seq)
        return

    for path in args:
        find_and_zip(path, opt.size, opt.zip_path, 'chunk_', opt.nozip)
