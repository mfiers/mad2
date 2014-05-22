from __future__ import print_function

import datetime
import logging
import sys
import os
import leip
import hashlib

from lockfile import FileLock

from mad2.util import get_all_mad_files
from mad2.hash import get_qdhash

lg = logging.getLogger(__name__)


def check_qdsumfile(qdfile, filename):
    if not os.path.exists(qdfile):
        return None
    with open(qdfile) as F:
        for line in F:
            hsh, fn = line.strip().split()
            if fn == filename:
                return hsh
    return None


def append_qdsumfile(qdfile, filename, qd):
    qds = {}

    with FileLock(qdfile):
        #read old qdfile
        if os.path.exists(qdfile):
            with open(qdfile) as F:
                for line in F:
                    hsh, fn = line.strip().split()
                    qds[fn] = hsh

        #insert our qd - possibly overwriting other version
        qds[filename] = qd

        #write new qdfile
        qds.keys
        with open(qdfile, 'w') as F:
            for fn in sorted(qds.keys()):
                F.write("{}  {}\n".format(qds[fn], fn))


@leip.hook("madfile_post_load", 250)
def qdhook(app, madfile):

    if madfile.get('orphan', False):
        # won't deal with orphaned files
        return
    if madfile.get('isdir', False):
        # won't deal with dirs
        return

    dirname = madfile['dirname']
    filename = madfile['filename']

    qdfile = os.path.join(dirname, 'QDSUMS')
    qd_file = check_qdsumfile(qdfile, filename)
    qd = get_qdhash(madfile['fullpath'])

    if qd_file is None:
        #qd does not exists yet - create & return
        append_qdsumfile(qdfile, filename, qd)
        madfile.all['qdhash'] = qd
        return

    #else - check if the qd has chenged
    if qd_file == qd:
        #no? All is well -
        return

    lg.warning("'%s' may have hanged (qd hash did)", madfile['inputfile'])
