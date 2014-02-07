# -*- coding: utf-8 -*-
from __future__ import print_function,  unicode_literals

from collections import Counter
import logging
import os
from signal import signal, SIGPIPE, SIG_DFL
import subprocess
import sys
import tempfile

from dateutil.parser import parse as dateparse

import leip
import Yaco
import mad2.ui

from mad2.util import  get_mad_file, get_all_mad_files

#Ignore SIG_PIPE and don't throw exceptions
#otherwise it crashes when you pipe into, for example, head
#see http://newbebweb.blogspot.be/2012/02/python-head-ioerror-errno-32-broken.html
#see http://docs.python.org/library/signal.html
signal(SIGPIPE, SIG_DFL)

lg = logging.getLogger(__name__)

def dispatch():
    """
    Run the app - this is the actual application entry point
    """
    app.run()


##
## define Mad commands
##


##
## define show
##
@leip.arg('-y', '--yaml', action='store_true')
@leip.arg('file', nargs='*')
@leip.command
def show(app, args):
    i = 0

    for madfile in get_all_mad_files(app, args):
        if i > 0:
            print('---')

        d = madfile.collapse()

        if args.yaml:
            print(d.pretty())
        else:
            for k in sorted(d.keys()):
                v = d[k]

                if isinstance(v, dict):
                    continue
                print("{}\t{}".format(k,v))

        i += 1


@leip.commandName('key')
def madkeylist(app, args):
    kyw = sorted(app.conf.keywords)
    mxlen = str(max([len(x) for x in kyw])+1)
    for c in sorted(app.conf.keywords):
        print(("{:<" + mxlen + "} {}").format(c, app.conf.keywords[c].description))


@leip.commandName('category')
def madcatlist(app, args):
    kyw = sorted(app.conf.keywords.category.allowed)
    mxlen = str(max([len(x) for x in kyw])+1)
    for c in sorted(kyw):
        dsc = app.conf.keywords.category.allowed[c]
        print(("{:<" + mxlen + "} {}").format(c, dsc))
        tmpl = app.conf.template[c]
        if not tmpl: continue
        mxlen2 = str(max([len(x) for x in tmpl])+1)
        for k in sorted(tmpl):
            print(("  - {:<" + mxlen2 + "} {}").format(k, app.conf.template[c][k]))


@leip.arg('section', help='section to print', default='', nargs='?')
@leip.command
def sysconf(app, args):
    c = app.conf[args.section]
    print(c.pretty())

@leip.arg('comm', metavar='command', help='command to check')
@leip.command
def has_command(app, args):
    comm = args.comm
    lg.debug("checking if we know command %s" % comm)
    if comm in app.leip_commands:
        sys.exit(0)
    else:
        sys.exit(-1)


##
## Instantiate the app and discover hooks & commands
##

#trail of config files???
config_files = [
    'pkg://mad2/etc/*.config',
    '/etc/mad2/',
    '~/.config/mad2/']

for c in config_files:
    lg.debug("using config file: {}".format(c))

app = leip.app(name='mad2', set_name="config",
               config_files = config_files)

#discover hooks in this module!
app.discover(globals())
