#!/usr/bin/env python

import os
import sys
import logging

from optparse import OptionParser
from bugalo import *

import zipfile
from pprint import pprint as pp

MB = (1024*1024)
log = logging.getLogger()

def find_and_zip(search_path, zsize, zpath='.', zprefix='', nozip=False):
    # This should be considered to be at the batch level
    # All files found are assumed to be from the same source export
    groups = find_groups(search_path)

    # Extract bundles and inject full_path and import_path
    bundles = []
    for group in groups:
        for bundle in group['bundles']:
            b = bundle
            b['full_path'] = group['full_path']
            b['import_path'] = group['import_path']
            bundles.append(b)

    chunk_size = MB * zsize
    chunks = chunkify_fifo(bundles, chunk_size)
    chunk_no = 0
    for chunk in chunks:
        chunk_no += 1 # Yes, we start at 1
        t_size = sum([b['size'] for b in chunk])
        log.info('Chunk %3d - %6.2f MB (%4d%% full)'%(chunk_no,
                                        (float(t_size)/float(MB)),
                                        (float(t_size)/float(chunk_size))*100))
        if nozip: continue
        zfile_path = os.path.join(zpath,'%s%03d.zip'%(zprefix,chunk_no))
        zfile = zipfile.ZipFile(zfile_path, 'w')
        for bundle in chunk:
            for f in bundle['files']:
                z_fp = os.path.join(bundle['full_path'],f['path'],f['name'])
                z_ip = os.path.join(bundle['import_path'],f['path'],f['name'])
                zfile.write(z_fp, z_ip)
        zfile.close()

def main():
    usage = "Usage: %prog [options] path [path, ...] "
    parser = OptionParser(usage=usage)
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
    (options, args) = parser.parse_args()

    # Setup logging
    log.setLevel(logging.WARN)
    log.addHandler(logging.StreamHandler())
    if options.verbose: log.setLevel(logging.INFO)
    if options.debug: log.setLevel(logging.DEBUG)

    if len(args) == 0:
        parser.print_usage()
        sys.exit(1)

    for path in args:
        find_and_zip(path,options.size,options.zip_path,'chunk_',options.nozip)
