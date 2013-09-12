from __future__ import print_function
import os
import logging
import socket
import leip
from pwd import getpwuid
import hashlib
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)

@leip.hook("madfile_load")
def onthefly(app, madfile):
    lg.debug("running onthelfy")
    madfile.otf.fullpath = os.path.abspath(madfile.filename)
    lg.debug("get fqdn")
    madfile.otf.host = socket.gethostname()
    filestat = os.stat(madfile.filename)
    madfile.otf.filesize = filestat.st_size
    userinfo = getpwuid(filestat.st_uid)
    madfile.otf.userid = userinfo.pw_name
    madfile.otf.username = userinfo.pw_gecos
    lg.debug("finished onthefly")

@leip.arg('file', nargs='*')
@leip.command
def qdhash(app, args):
    for madfile in get_all_mad_files(app, args):
        print("{0}  {1}".format(qd_hash(madfile.filename), madfile.filename))