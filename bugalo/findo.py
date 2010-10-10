#!/usr/bin/env python

import os
import sys
import logging

from optparse import OptionParser
from bugalo import *

import zipfile
from pprint import pprint as pp

def main():
    usage = "Usage: %prog [options] path [path, ...] "
    parser = OptionParser(usage=usage)
    parser.add_option("-z", "--zip-path", dest="zip_path", help="zip output path",
                        default = '.')
    parser.add_option("-c", "--chunk-size", dest="size", type="int", default=10,
                        help="chunk size in MB")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                        default=False, help="turn on info messages")
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
                        default=False, help="turn on debugging")
    parser.add_option("-n", "--no-zip", dest="nozip", action="store_true",
                        default=False, help="dry run")
    (options, args) = parser.parse_args()

    # Setup logging
    log = logging.getLogger()
    log.setLevel(logging.WARN)
    log.addHandler(logging.StreamHandler())
    if options.verbose: log.setLevel(logging.INFO)
    if options.debug: log.setLevel(logging.DEBUG)

    if len(args) == 0:
        parser.print_usage()
        sys.exit(1)

    MB = (1024*1024)

    for search_path in args:
        groups = find_groups(search_path, search_path)
        pp(groups)

        # Extract bundles and inject full_path and import_path
        bundles = []
        for group in groups:
            for bundle in group['bundles']:
                b = bundle
                b['full_path'] = group['full_path']
                b['import_path'] = group['import_path']
                bundles.append(b)

        chunk_size = MB * options.size
        chunks = chunkify_fifo(bundles, chunk_size)
        chunk_no = 0
        for chunk in chunks:
            chunk_no += 1 # Yes, we start at 1
            t_size = sum([b['size'] for b in chunk])
            #t_size = 0
            #for bundle in chunk:
            #    t_size += bundle['total_size']
            log.info('Chunk %3d - %6.2f MB (%4d%% full)'%(chunk_no,
                                            (float(t_size)/float(MB)),
                                            (float(t_size)/float(chunk_size))*100))
            if options.nozip: continue
            zfile_path = os.path.join(options.zip_path,'chunk_%03d.zip'%chunk_no)
            zfile = zipfile.ZipFile(zfile_path, 'w')
            for bundle in chunk:
                for f in bundle['files']:
                    z_fp = os.path.join(bundle['full_path'],f['path'],f['name'])
                    z_ip = os.path.join(bundle['import_path'],f['path'],f['name'])
                    zfile.write(z_fp, z_ip)
            zfile.close()
