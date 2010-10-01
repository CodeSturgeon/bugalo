#!/usr/bin/env python

import os
import logging

from optparse import OptionParser
from bugalo import *

import zipfile
from pprint import pprint as pp

def main():
    parser = OptionParser()
    parser.add_option("-s", "--source", dest="source", help="source path",
                        default = '/home/flute/for_fish/20100914/')
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


    log = logging.getLogger()
    log.setLevel(logging.WARN)
    log.addHandler(logging.StreamHandler())
    if options.verbose: log.setLevel(logging.INFO)
    if options.debug: log.setLevel(logging.DEBUG)

    search_path = options.source
    MB = (1024*1024)

    packs = []

    packs = find_packs(search_path, search_path)
    chunk_size = MB * options.size
    chunks = chunkify_fifo(chunk_size,packs)
    chunk_no = 0
    for chunk in chunks:
        chunk_no += 1 # Yes, we start at 1
        t_size = 0
        for pack in chunk:
            t_size += pack['total_size']
        log.info('Chunk %3d - %6.2f MB (%4d%% full)'%(chunk_no,
                                        (float(t_size)/float(MB)),
                                        (float(t_size)/float(chunk_size))*100))
        if options.nozip: continue
        zfile_path = os.path.join(options.zip_path,'chunk_%03d.zip'%chunk_no)
        zfile = zipfile.ZipFile(zfile_path, 'w')
        for pack in chunk:
            for f in pack['files']:
                zfile.write(os.path.join(pack['full_path'],f['path'],f['name']),
                            os.path.join(pack['import_path'],f['path'],f['name']))
        zfile.close()
