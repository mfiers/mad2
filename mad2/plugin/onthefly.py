from __future__ import print_function
import os
import logging
import socket
import leip
from pwd import getpwuid
import hashlib
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)
#lg.setLevel(logging.DEBUG)

def qd_hash(filename):
    """
    Provde a quick & dirty hash

    this is by no means secure, but quick for very large files, and as long
    as one does not try to create duplicate hashes, the chance is still very
    slim that a duplicate will arise
    """
    sha1sum = hashlib.sha1()
    filesize = os.stat(filename).st_size
    if filesize < 10000:
        with open(filename) as F:
            sha1sum.update(F.read().encode())
    else:
        with open(filename) as F:
            sha1sum.update(F.read(2000))
            F.seek(int(filesize * 0.4))
            sha1sum.update(F.read(2000))
            F.seek(-2000, 2)
            sha1sum.update(F.read())
    return sha1sum.hexdigest()

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
    madfile.otf.qdhash = qd_hash(madfile.filename)
    lg.debug("finished onthefly")

@leip.arg('file', nargs='*')
@leip.command
def qdhash(app, args):
    for madfile in get_all_mad_files(app, args):
        print("{0}  {1}".format(qd_hash(madfile.filename), madfile.filename))