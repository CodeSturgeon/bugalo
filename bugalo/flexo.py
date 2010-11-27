#!/usr/bin/env python

import os
import re
import sys
import logging
import datetime
import couchdb

from optparse import OptionParser
from bugalo import *

from pprint import pprint as pp

MB = (1024*1024)
log = logging.getLogger()

def main():
    usage = "Usage: %prog [options] path [path, ...] "
    parser = OptionParser(usage=usage)
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true",
                        default=False, help="turn on debugging")
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
                        default=False, help="turn on debugging")
    (opt, args) = parser.parse_args()

    # Setup logging
    log.setLevel(logging.INFO)
    log.addHandler(logging.StreamHandler())
    if opt.quiet: log.setLevel(logging.WARN)
    if opt.debug: log.setLevel(logging.DEBUG)

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
        source_folders = os.listdir(batch_path)
        for src in source_folders:
            # Ignore dot files
            if src.startswith('.'): continue
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

    db.save({
        'date': now_date,
        'sources': sources
    })
