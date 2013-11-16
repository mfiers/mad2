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

    filename = madfile.all.filename
    madname = madfile.all.madname

    if os.path.exists(madname):
        fstats = os.stat(filename)
        os.chmod(madname, fstats.st_mode)
        os.chown(madname, fstats.st_uid, fstats.st_gid)
