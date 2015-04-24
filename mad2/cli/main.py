# -*- coding: utf-8 -*-


import logging
from datetime import datetime
from signal import signal, SIGPIPE, SIG_DFL
import os
import sys

os.environ['TERM'] = 'xterm' #prevent weird output excape code

import arrow
import leip
from termcolor import colored

import mad2.ui
import mad2.util

# Ignore SIG_PIPE and don't throw exceptions
# otherwise it crashes when you pipe into, for example, head
# see http://tinyurl.com/nwpwyoj
# see http://docs.python.org/library/signal.html
signal(SIGPIPE, SIG_DFL)


lg = logging.getLogger(__name__)


PROFILE = False

def dispatch():
    """
    Run the app - this is the actual application entry point
    """
    if PROFILE:
        import cProfile
        import os
        import pstats
        import tempfile

        pr = cProfile.Profile()
        pr.enable()
        app.run()
        pr.disable()
        handle = tempfile.NamedTemporaryFile(
            delete=False, dir=os.getcwd(), prefix='Mad2.', suffix='.profiler')
        sortby = 'tottime'
        ps = pstats.Stats(pr, stream=handle).sort_stats(sortby)
        ps.print_stats()
        handle.close()
    else:
        app.run()


#
# define show
#
@leip.arg('file', nargs='*')
@leip.flag('-r', '--raw', help='output yaml representation')
@leip.flag('-t', '--tsv', help='output tab delimited representation '
          + '(top level only)')
@leip.command
def show(app, args):
    """ Show mad annotation of one or more file(s)

    default shows a screen formatted output
    """


    i = 0

    if args.tsv:
        for madfile in mad2.util.get_all_mad_files(app, args):
            if i > 0:
                print('---')

            for k in sorted(madfile.keys()):
                v = madfile[k]

                v = _format_value(madfile[k])

            i += 1
    elif args.raw:
        for madfile in mad2.util.get_all_mad_files(app, args):
            x = madfile.all.copy()
            x.update(madfile.mad)
            print(x.pretty())

    else:
        for madfile in mad2.util.get_all_mad_files(app, args):
            print_nicely(app, madfile)

def _format_value(v):
    if isinstance(v, dict):
        return '...'
    elif isinstance(v, list):
        return v[:5]
    elif isinstance(v, datetime):
        return str(arrow.get(v).to('local'))
    else:
        return str(v)

def print_nicely(app, madfile):
    implicit = []
    explicit = []
    max_key_len = 0
    for k in list(madfile.keys()):
        max_key_len = max(len(k), max_key_len)
        v = _format_value(madfile[k])
        if k in madfile.mad:
            explicit.append((k,v))
        else:
            implicit.append((k,v))

    fs = '{} {}: {}'
    ffs = '%-' + str(max_key_len) + 's'

    for k, v in sorted(explicit):
        if k in ['_id', '_id_dump']:
            continue

        print(fs.format(
              colored('x', 'red'),
              ffs % k,
              v))

    for k, v in sorted(implicit):
        print(fs.format(
              colored('i', 'cyan'),
              ffs % k,
              v))


@leip.commandName('key')
def madkeylist(app, args):
    kyw = sorted(app.conf.keywords)
    mxlen = str(max([len(x) for x in kyw]) + 1)
    for c in sorted(app.conf.keywords):
        print(("{:<" + mxlen + "} {}")
              .format(c, app.conf.keywords[c].description))


@leip.commandName('category')
def madcatlist(app, args):
    kyw = sorted(app.conf.keywords.category.allowed)
    mxlen = str(max([len(x) for x in kyw]) + 1)
    for c in sorted(kyw):
        dsc = app.conf.keywords.category.allowed[c]
        print(("{:<" + mxlen + "} {}").format(c, dsc))
        tmpl = app.conf.template[c]
        if not tmpl:
            continue
        mxlen2 = str(max([len(x) for x in tmpl]) + 1)
        for k in sorted(tmpl):
            print(("  - {:<" + mxlen2 + "} {}")
                  .format(k, app.conf.template[c][k]))


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


#
# Instantiate the app and discover hooks & commands
#
#logging.getLogger('leip').setLevel(logging.DEBUG)

app = leip.app('mad2')

# app.parser.add_argument('--trust', action='store_true', help='trust sha1sum')
# discover hooks in this module!
app.discover(globals())
