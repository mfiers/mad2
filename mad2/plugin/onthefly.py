from __future__ import print_function, division

from datetime import datetime
import os
import logging
import socket

import leip
import fantail
from pwd import getpwuid

lg = logging.getLogger(__name__)

EXTENSION_DATA = None
RECURSE_CACHE = {}


def get_fiex(app):
    global EXTENSION_DATA
    if not EXTENSION_DATA is None:
        return EXTENSION_DATA

    EXTENSION_DATA = {}

    for ft_name in app.conf['filetype']:
        ft = app.conf['filetype'][ft_name]
        for ext in ft.get('extensions', []):
            EXTENSION_DATA[ext] = ft_name, ft

    return EXTENSION_DATA


def apply_file_format(app, madfile, filename=None):

    extension_data = get_fiex(app)

    if filename is None:
        filename = madfile['filename']

    splitter = filename.rsplit('.', 1)
    if len(splitter) != 2:
        return

    base, ext = splitter

    # this ensures that the innermost extension seen is stored
    madfile.all['extension'] = ext

    if not ext in extension_data:
        return

    filetype, ftinfo = extension_data[ext]
    lg.debug("identified filetype {0}".format(filetype))
    template_name = ftinfo.get('template')

    template = app.conf['template.{0}'.format(template_name)]
    madfile.all.update(template)
    if ftinfo.get('continue', False):
        lg.debug("contiue filetype disocvery on: %s" % base)
        apply_file_format(app, madfile, base)


@leip.hook("madfile_pre_load")
def recursive_dir_data(app, madfile):

    global RECURSE_CACHE

    lg.debug("start pre load for {}".format(madfile['filename']))
    here = madfile['dirname'].rstrip('/')
    conf = []

    # find existing directory configuration from the perspective
    # of the madfile
    last = here
    while True:
        try:
            assert(os.path.isdir(here))
        except AssertionError:
            print(last, here)
            print(madfile.pretty())
            raise
        here_c = os.path.join(here, '.mad', 'config')
        if os.path.exists(here_c):
            conf.append(here_c)

        parent = os.path.dirname(here)
        if parent == '/':  # no config in the root - that would be evil!
            break
        last = here
        here = parent

    # now again from the current directory
    here = os.getcwd().rstrip('/')
    last = here
    cwdconf = []
    while True:
        try:
            assert(os.path.isdir(here))
        except:
            print(last, here)
            print(madfile.pretty())
            raise
        here_c = os.path.join(here, '.mad', 'config')
        if os.path.exists(here_c):
            if here_c in conf:
                break  # overlap with tree from the madfile's location
            else:
                cwdconf.append(here_c)
        parent = os.path.dirname(here)
        if parent == '/':  # no config in the root - that would be evil!
            break
        last = here
        here = parent

    conf = cwdconf + conf
    # load (or get from cache)
    for c in conf[::-1]:
        fullname = os.path.expanduser(os.path.abspath(c))
        if fullname in RECURSE_CACHE:
            y = RECURSE_CACHE[fullname]
        else:
            y = fantail.dir_loader(fullname)
            RECURSE_CACHE[fullname] = y

        # insert in the stack just after the mad file
        madfile.all.update(y)


@leip.hook("madfile_post_load")
def onthefly(app, madfile):

    # if sorted(list(madfile.keys())) == ['hash']:
    #     madfile.all['annotated'] = False
    # else:
    #     madfile.all['annotated'] = True

    lg.debug("running onthelfy")
    madfile.all['fullpath'] = os.path.abspath(madfile['filename'])
    madfile.all['fullmadpath'] = os.path.abspath(madfile['madname'])

    lg.debug("get fqdn")
    madfile.all['host'] = socket.gethostname()

    madfile.all['uri'] = "file://{}{}".format(
        madfile.all['host'], madfile['fullpath'])

    if madfile.get('orphan', False):
        # orphaned is file - little we can do
        return

#    if not os.path.exists(madfile['fullpath'])
    filestat = os.stat(madfile['fullpath'])
    # print(filestat)

    madfile.all['filesize'] = filestat.st_size
    madfile.all['nlink'] = filestat.st_nlink

    try:
        userinfo = getpwuid(filestat.st_uid)
    except KeyError:
        # cannot find username based on uid
        madfile.all['userid'] = str(filestat.st_uid)
        madfile.all['username'] = str(filestat.st_uid)
    else:
        madfile.all['userid'] = userinfo.pw_name
        madfile.all['username'] = userinfo.pw_gecos

    mtime = datetime.utcfromtimestamp(
        filestat.st_mtime)
    atime = datetime.utcfromtimestamp(
        filestat.st_atime)

    madfile.all['atime'] = atime.isoformat()
    madfile.all['atime_simple'] = atime.strftime("%Y/%m/1")

    madfile.all['mtime'] = mtime.isoformat()
    madfile.all['mtime_simple'] = mtime.strftime("%Y/%m/1")

    apply_file_format(app, madfile)

    lg.debug("finished onthefly")
