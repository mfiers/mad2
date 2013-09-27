from __future__ import print_function

from datetime import datetime
import os
import logging
import socket

import leip
from pwd import getpwuid

lg = logging.getLogger(__name__)

EXTENSION_DATA = None


def get_fiex(app):
    global EXTENSION_DATA
    if not EXTENSION_DATA is None:
        return EXTENSION_DATA

    EXTENSION_DATA = {}
    for ft in app.conf.filetype:
        for ext in app.conf.filetype.get(ft).extensions:
            EXTENSION_DATA[ext] = ft

    return EXTENSION_DATA


def apply_file_format(app, madfile, filename=None):

    extension_data = get_fiex(app)

    if filename is None:
        filename = madfile.otf.basename

    splitter = filename.rsplit('.', 1)
    if len(splitter) != 2:
        return

    base, ext = splitter

    #this ensures that the innermost extension seen is stored
    madfile.otf.extension = ext

    if not ext in extension_data:
        return

    ft = extension_data[ext]
    ftinfo = app.conf.filetype.get(ft)

    lg.debug("identified filetype %s" % ft)
    if ftinfo.template:
        template = app.conf.template.get(ftinfo.template)
        madfile.otf.update(template)

    if ftinfo.get('continue', False):
        lg.debug("contiue filetype disocvery on: %s" % base)
        apply_file_format(app, madfile, base)


@leip.hook("madfile_load")
def onthefly(app, madfile):

    if sorted(list(madfile.mad.keys())) == ['hash']:
        madfile.otf.annotated = False
    else:
        madfile.otf.annotated = True

    lg.debug("running onthelfy")
    madfile.otf.fullpath = os.path.abspath(madfile.filename)
    lg.debug("get fqdn")
    madfile.otf.host = socket.gethostname()

    filestat = os.stat(madfile.filename)
    madfile.otf.filesize = filestat.st_size
    userinfo = getpwuid(filestat.st_uid)
    madfile.otf.userid = userinfo.pw_name
    madfile.otf.username = userinfo.pw_gecos

    mtime = datetime.utcfromtimestamp(
        filestat.st_mtime)
    atime = datetime.utcfromtimestamp(
        filestat.st_atime)

    madfile.otf.atime = atime.isoformat()
    madfile.otf.atime_simple = atime.strftime("%m%y")

    madfile.otf.mtime = mtime.isoformat()
    madfile.otf.mtime_simple = mtime.strftime("%m%y")

    apply_file_format(app, madfile)

    lg.debug("finished onthefly")
