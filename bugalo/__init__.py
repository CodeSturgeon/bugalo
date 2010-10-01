import os
from os.path import isfile
from hashlib import md5, sha1
import logging

log = logging.getLogger()

def make_pack(path, search_path):
    meta = {}

    meta['full_path'] = path
    meta['import_path'] = path.split(search_path)[1].lstrip('/\\')
    meta['folder'] = os.path.split(path)[1]

    meta['files'] = []
    meta['total_size'] = 0
    for root, dirs, files in os.walk(path):
        for entity in files:
            if entity[0] == '.': next
            file_path = os.path.join(root,entity)
            file_cont = open(file_path, 'rb').read()
            file_meta = {}
            file_meta['name'] = entity
            file_meta['path'] = root.split(path)[1].lstrip('/\\')
            file_meta['md5'] = md5(file_cont).hexdigest()
            file_meta['sha1'] = sha1(file_cont).hexdigest()
            file_meta['size'] = os.path.getsize(file_path)
            meta['total_size'] += file_meta['size']
            meta['files'].append(file_meta)
    return meta

def find_packs(path, search_path):
    log.debug('looking for packs in %s'%path)
    entities = os.listdir(path)

    # Remove dot files from the listing
    entities = filter(lambda fname: fname[0]!='.', entities)
    
    # Look for files in the dir contents and make pack if found
    file_finder = lambda last, fname: last or isfile(os.path.join(path,fname))
    if reduce(file_finder, entities, False):
        log.debug('pack found in %s'%path)
        return [make_pack(path,search_path)]

    # No files found, recurse on all directories
    packs = []
    for dname in entities:
        ret = find_packs(os.path.join(path,dname),search_path)
        if len(ret)>0: packs.extend(ret) # Ignore empty results

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

