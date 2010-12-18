import os
from os.path import isfile
from hashlib import md5, sha1
import zipfile
import logging

log = logging.getLogger()
MB = (1024*1024)
max_bundle_size = 500 * MB

def find_and_zip(search_path, zsize, zpath='.', zprefix='', nozip=False, seq=0):
    # This should be considered to be at the batch level
    # All files found are assumed to be from the same source export
    # seq is the number of the previous file in the sequence
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
    chunk_no = seq
    for chunk in chunks:
        chunk_no += 1
        t_size = sum([b['size'] for b in chunk])
        log.info('%s%03d - %6.2f MB (%4d%% full)'%(zprefix, chunk_no,
                                        (float(t_size)/float(MB)),
                                        (float(t_size)/float(chunk_size))*100))
        if nozip: continue
        zfile_path = os.path.join(zpath,'%s%03d.zip'%(zprefix,chunk_no))
        zfile = zipfile.ZipFile(zfile_path, 'w')
        for bundle in chunk:
            for f in bundle['files']:
                z_fp = os.path.join(bundle['full_path'],f['path'],f['name'])
                z_ip = os.path.join(bundle['import_path'],f['path'],f['name'])
                try:
                    zfile.write(z_fp, z_ip)
                except Exception, e:
                    import time
                    log.error('trying to zip: %s'%z_fp)
                    log.error(os.stat(z_fp))
                    log.error('waiting a moment')
                    time.sleep(5)
                    log.error(os.stat(z_fp))
                    log.error('touching file')
                    # http://technet.microsoft.com/en-us/library/bb490886.aspx
                    cmd = 'copy /b '+z_fp+'+,,'
                    log.error(cmd)
                    ret = os.system(cmd)
                    log.error('ret: %s'%ret)
                    log.error(os.stat(z_fp))
                    log.error('waiting a moment')
                    time.sleep(5)
                    log.error(os.stat(z_fp))
                    raise e
        zfile.close()

    # Return the last chunk_no for use in next sequence
    return chunk_no

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

