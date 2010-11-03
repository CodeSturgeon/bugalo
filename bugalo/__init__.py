import os
from os.path import isfile
from hashlib import md5, sha1
import logging

log = logging.getLogger()
MB = (1024*1024)
max_bundle_size = 500 * MB

def make_group(path, base_path):
    meta = {}

    meta['full_path'] = path
    meta['import_path'] = path.split(base_path)[1].lstrip('/\\')
    meta['folder'] = os.path.split(path)[1]

    group_files = []
    total_size = 0
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
            total_size += file_meta['size']
            group_files.append(file_meta)

    bundles = []
    if total_size < max_bundle_size:
        bundles = [{'size':total_size, 'files':group_files}]
    else:
        chunks = chunkify_fifo(group_files)
        for chunk in chunks:
            total = sum([f['size'] for f in chunk])
            bundles.append({'size':total, 'files':chunk})

    meta['bundles'] = bundles

    return meta

# FIXME can I base_path=path here? path should be base_path... or is that base_path?
def find_groups(path, base_path=None):
    log.debug('looking for group in %s'%path)

    if base_path == None: base_path = path

    entities = os.listdir(path)

    # Remove dot files from the listing
    entities = filter(lambda fname: fname[0]!='.', entities)
    
    # Look for files in the dir contents and make group if found
    # FIXME this could be improved
    file_finder = lambda last, fname: last or isfile(os.path.join(path,fname))
    if reduce(file_finder, entities, False):
        log.debug('group found in %s'%path)
        return [make_group(path,base_path)]

    # No files found, recurse on all directories
    groups = []
    for dname in entities:
        ret = find_groups(os.path.join(path,dname),base_path)
        if len(ret)>0: groups.extend(ret) # Ignore empty results

    return groups

def chunkify_sprinkle(no_chunks, fgroup_list):
    chunks = Array([]*no_chunks)
    spin = 0
    for fg in fgroup_list:
        chunks[spin].append(fg)
        spin += 1
        if spin == no_chunks: spin = 0
    return chunks

def chunkify_fifo(item_list, max_size = 500*MB ):
    chunks = []
    chunk = []
    chunk_size = 0
    for item in item_list:
        log.debug("I - %s"%item)
        try:
            item_size = int(item['size'])
        except Exception, e:
            log.error(item_list)
            log.error(item)
            raise e
        if item_size > max_size:
            log.warn('item size > chunk size - %s'%item['full_path'])
        if chunk_size + item_size > max_size and len(chunk) > 0:
            chunks.append(chunk)
            chunk = []
            chunk_size = 0
        chunk.append(item)
        chunk_size += item_size
    chunks.append(chunk) # Catch the last chunk
    return chunks

