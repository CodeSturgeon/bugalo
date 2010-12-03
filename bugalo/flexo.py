#!/usr/bin/env python

import os
import re
import sys
import logging
import datetime
import json
import couchdb

from optparse import OptionParser
from bugalo import *

from pprint import pprint as pp

MB = (1024*1024)
log = logging.getLogger()

def main():
    usage = "Usage: %prog [options] path [path, ...] "
    parser = OptionParser(usage=usage)
    parser.add_option("-o", "--output", dest="output", action="store_true",
                        default=False, help="Output JSON to files, not couch")
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true",
                        default=False, help="Turn off all information messages")
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
                        default=False, help="Turn on debugging")
    (opt, args) = parser.parse_args()

    # Setup logging
    log.setLevel(logging.INFO)
    log.addHandler(logging.StreamHandler())
    if opt.quiet: log.setLevel(logging.WARN)
    if opt.debug: log.setLevel(logging.DEBUG)

    if opt.output:
        # Global counter for files
        file_seq = 0
        # File prefix for this run
        file_pre = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    else:
        # FIXME add cmd config for this
        couch = couchdb.Server()
        db = couch['flexo']

    if len(args) == 0:
        parser.print_usage()
        sys.exit(1)

    # FIXME convert to ISO
    now_date = datetime.datetime.now().strftime('%Y%m%d')
    batch_re = re.compile('(?P<name>.*?)_(?P<date>.*)')
    sources = []
    for batch_path in args:
        log.info('BATCH: %s'%batch_path)
        source_folders = os.listdir(batch_path)
        for src in source_folders:
            # Ignore dot files
            if src.startswith('.'): continue
            log.info('SOURCE: %s'%src)
            abs_src = os.path.join(batch_path,src)
            if os.path.isfile(abs_src):
                log.warn('%s is not a folder - skipping'%abs_src)
                continue
            try:
                vendor, src_date = batch_re.match(src).groups()
            except AttributeError:
                log.warn('%s is not named correctly - skipping'%abs_src)
                continue
            groups = find_groups(abs_src)
            sources.append({
                'vendor': vendor,
                'date': src_date,
                'groups': groups,
            })

        doc = { 'date': now_date, 'sources': sources }
        if opt.output:
            with open('%s-%s.json'%(file_pre,file_seq),'w') as f:
                f.write(json.dumps(doc, indent=2))
            file_seq += 1
        else:
            ret = db.save(doc)
            # check ret and output new doc URL
