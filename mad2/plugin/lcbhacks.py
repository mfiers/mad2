from __future__ import print_function

import logging
import leip

from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)

@leip.hook("madfile_load", 100)
def lbconthefly(app, madfile):

    fullpath = madfile.otf.fullpath
    if '/media/seq-srv-04' in fullpath:
        madfile.otf.host = 'SEQ_SRV_04'
    if '/media/seq-srv-05' in fullpath:
        madfile.otf.host = 'SEQ_SRV_05'
    if '/media/seq-srv-06' in fullpath:
        madfile.otf.host = 'SEQ_SRV_06'
