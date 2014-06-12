from __future__ import print_function

import logging
import leip
import os
import re

from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)


@leip.hook("madfile_post_save", 100)
def root(app, madfile):

    if os.geteuid() != 0:
        #only do something when root
        return

    lg.debug("changing .madfile permission")

    filename = madfile['inputfile']
    madname = madfile['madname']

    if os.path.exists(madname):
        fstats = os.stat(filename)
        os.chmod(madname, fstats.st_mode)
        os.chown(madname, fstats.st_uid, fstats.st_gid)

    #change SHA1SUM file
    dirname = madfile['dirname']
    sha1name = os.path.join(dirname, 'SHA1SUMS')
    qdname = os.path.join(dirname, 'QDSUMS')

    dstats = os.stat(dirname)

    if os.path.exists(sha1name):
        os.chmod(sha1name, 0o664)
        os.chown(sha1name, dstats.st_uid, dstats.st_gid)
    if os.path.exists(qdname):
        os.chmod(qdname, 0o664)
        os.chown(qdname, dstats.st_uid, dstats.st_gid)

