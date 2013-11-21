from __future__ import print_function, division

from datetime import datetime
import os
import logging
import socket

import Yaco
import leip
from pwd import getpwuid

lg = logging.getLogger(__name__)

EXTENSION_DATA = None
RECURSE_CACHE = {}

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
        filename = madfile.all.basename

    splitter = filename.rsplit('.', 1)
    if len(splitter) != 2:
        return

    base, ext = splitter

    #this ensures that the innermost extension seen is stored
    madfile.all.extension = ext

    if not ext in extension_data:
        return

    ft = extension_data[ext]
    ftinfo = app.conf.filetype.get(ft)

    lg.debug("identified filetype %s" % ft)
    if ftinfo.template:
        template = app.conf.template.get(ftinfo.template)
        madfile.all.update(template)

    if ftinfo.get('continue', False):
        lg.debug("contiue filetype disocvery on: %s" % base)
        apply_file_format(app, madfile, base)


@leip.hook("madfile_pre_load")
def recursive_dir_data(app, madfile):
    lg.debug("start pre load for {}".format(madfile.all.filename))
    here = madfile.all.dirname.rstrip('/')
    conf = []

    #print ('x' * 80)
    #find existsing configurations
    last = here
    while True:
        try:
            assert(os.path.isdir(here))
        except AssertionError:
            print(last, here)
            print(madfile.all.pretty())
            raise
        here_c = os.path.join(here, '.mad', 'config')
        if os.path.exists(here_c):
            conf.append(here_c)

        parent = os.path.dirname(here)
        if parent == '/': #no config in the root - that would be evil!
            break
        last = here
        here = parent


    #load (or get from cache)
    for c in conf[::-1]:
        fullname = os.path.expanduser(os.path.abspath(c))
        if fullname in RECURSE_CACHE:
            y = RECURSE_CACHE[fullname]
        else:
            y = Yaco.YacoDir(fullname)
        madfile.all.update(y)

@leip.hook("madfile_post_load")
def onthefly(app, madfile):

    if sorted(list(madfile.mad.keys())) == ['hash']:
        madfile.all.annotated = False
    else:
        madfile.all.annotated = True

    lg.debug("running onthelfy")
    madfile.all.fullpath = os.path.abspath(madfile.filename)
    madfile.all.fullmadpath = os.path.abspath(madfile.madname)

    lg.debug("get fqdn")
    madfile.all.host = socket.gethostname()

    if madfile.orphan:
        #if file is orphaned - little we can do
        return

    filestat = os.stat(madfile.all.fullpath)
    madfile.all.filesize = filestat.st_size
    userinfo = getpwuid(filestat.st_uid)
    madfile.all.userid = userinfo.pw_name
    madfile.all.username = userinfo.pw_gecos

    mtime = datetime.utcfromtimestamp(
        filestat.st_mtime)
    atime = datetime.utcfromtimestamp(
        filestat.st_atime)

    madfile.all.atime = atime.isoformat()
    madfile.all.atime_simple = atime.strftime("%Y/%m/1")

    madfile.all.mtime = mtime.isoformat()
    madfile.all.mtime_simple = mtime.strftime("%Y/%m/1")

    apply_file_format(app, madfile)

    lg.debug("finished onthefly")