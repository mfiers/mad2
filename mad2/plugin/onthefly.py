from __future__ import print_function

import os
import logging
import socket
import sys

import leip
from pwd import getpwuid

from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)

FIEX = None

def get_fiex(app):
    global FIEX
    if not FIEX is None:
        return FIEX

    FIEX = {}
    for ft in app.conf.filetype:
        for ext in app.conf.filetype.get(ft).extensions:
            FIEX[ext] = ft

def apply_file_format(app, madfile, filename=None):

    fiex = get_fiex(app)

    if filename is None:
        filename = madfile.otf.basename

    splitter = filename.rsplit('.',1)
    if len(splitter) != 2:
        return

    base, ext = splitter

    #this ensures that the innermost extension seen is stored
    madfile.otf.extension = ext

    if not ext in FIEX:
        return

    ft = FIEX[ext]
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
    lg.debug("running onthelfy")
    madfile.otf.fullpath = os.path.abspath(madfile.filename)
    lg.debug("get fqdn")
    madfile.otf.host = socket.gethostname()
    filestat = os.stat(madfile.filename)
    madfile.otf.filesize = filestat.st_size
    userinfo = getpwuid(filestat.st_uid)
    madfile.otf.userid = userinfo.pw_name
    madfile.otf.username = userinfo.pw_gecos

    apply_file_format(app, madfile)

    lg.debug("finished onthefly")

@leip.arg('file', nargs='*')
@leip.command
def qdhash(app, args):
    for madfile in get_all_mad_files(app, args):
        print("{0}  {1}".format(qd_hash(madfile.filename), madfile.filename))