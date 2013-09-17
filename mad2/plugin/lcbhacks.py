from __future__ import print_function

import logging
import leip
import os
import re

from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)

@leip.hook("madfile_load", 100)
def lbconthefly(app, madfile):

    fullpath = madfile.otf.fullpath
    mtcher = re.compile(r'/media/(seq-srv-[0-9][0-9])', re.I)
    mtch = mtcher.search(fullpath)
    if not mtch:
        return


    server = mtch.groups()[0]

    madfile.otf.host = server.upper()
    fp = madfile.otf.fullpath
    fp = fp[fp.index(server) + len(server):]
    dn = os.path.dirname(fp)
    madfile.otf.fullpath = fp
    madfile.otf.dirname = dn
