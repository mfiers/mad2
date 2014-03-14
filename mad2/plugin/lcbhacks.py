from __future__ import print_function

import logging
import leip
import os
import re

lg = logging.getLogger(__name__)

@leip.hook("madfile_post_load", 100)
def lbconthefly(app, madfile):

    fullpath = madfile['fullpath']
    mtcher = re.compile(r'/media/(seq-srv-[0-9][0-9])', re.I)
    mtch = mtcher.search(fullpath)
    if not mtch:
        return


    server = mtch.groups()[0]

    madfile['host'] = server.upper()
