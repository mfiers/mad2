

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

    #TDDO: remove this later - bugfix
    if 'host' in madfile.mad:
        del madfile.mad['host']
    madfile.all['host'] = server.upper()
