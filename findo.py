#!/usr/bin/env python

import os
import logging
from hashlib import md5, sha1

log = logging.getLogger()
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

search_path = '/home/flute/for_fish/20100914/kaspT20_2010-09-13/'

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
        if chunk_size + fg_size > max_size:
            chunks.append(chunk)
            chunk = []
            chunk_size = 0
        chunk.append(fg)
        chunk_size += fg_size
    return chunks


import pprint as pp
packs = find_packs(search_path)
chunks = chunkify_fifo((1024*1024),packs)
for chunk in chunks:
    print 'x'*50
    pp.pprint(chunks)
