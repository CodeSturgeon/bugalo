#!/usr/bin/env python
# test2

import os
import logging
from hashlib import md5, sha1

from optparse import OptionParser
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

def make_pack(path):
    meta = {}

    meta['full_path'] = path
    meta['import_path'] = path.split(search_path)[1].lstrip('/\\')
    meta['folder'] = os.path.split(path)[1]

    meta['files'] = []
    meta['total_size'] = 0
    for entity in os.listdir(path):
        if entity[0] == '.': next
        file_path = os.path.join(path,entity)
        file_cont = open(file_path, 'rb').read()
        file_meta = {}
        file_meta['name'] = entity
        file_meta['md5'] = md5(file_cont).hexdigest()
        file_meta['sha1'] = sha1(file_cont).hexdigest()
        file_meta['size'] = os.path.getsize(file_path)
        meta['total_size'] += file_meta['size']
        meta['files'].append(file_meta)
    return meta

def find_packs(path):
    log.debug('looking for packs in %s'%path)
    packs = []
    for root, dirs, files in os.walk(path):
        log.debug('sniffing %s'%root)
        if len(dirs) and len(files):
            log.warn("yucking %s"%root)
        elif len(files):
            log.debug('pack found in %s'%root)
            packs.append(make_pack(root))
    return packs

def chunkify_sprinkle(no_chunks, fgroup_list):
    chunks = Array([]*no_chunks)
    spin = 0
    for fg in fgroup_list:
        chunks[spin].append(fg)
        spin += 1
        if spin == no_chunks: spin = 0
    return chunks

def chunkify_fifo(max_size, fgroup_list):
    chunks = []
    chunk = []
    chunk_size = 0
    for fg in fgroup_list:
        fg_size = int(fg['total_size'])
        if fg_size > max_size:
            log.warn('file group > chunk size - %s'%fg['full_path'])
        if chunk_size + fg_size > max_size and len(chunk) > 0:
            chunks.append(chunk)
            chunk = []
            chunk_size = 0
        chunk.append(fg)
        chunk_size += fg_size
    return chunks

import zipfile
import pprint as pp
packs = find_packs(search_path)
chunk_size = MB * options.size
chunks = chunkify_fifo(chunk_size,packs)
chunk_no = 0
for chunk in chunks:
    chunk_no += 1 # Yes, we start at 1
    t_size = 0
    for group in chunk:
        t_size += group['total_size']
    log.info('Chunk %3d - %6.2f MB (%4d%% full)'%(chunk_no,
                                    (float(t_size)/float(MB)),
                                    (float(t_size)/float(chunk_size))*100))
    if options.nozip: continue
    zfile = zipfile.ZipFile(os.path.join(options.zip_path,'chunk_%03d.zip'%chunk_no),'w')
    for group in chunk:
        for f in group['files']:
            zfile.write(os.path.join(group['full_path'],f['name']),os.path.join(group['import_path'],f['name']))
    zfile.close()
