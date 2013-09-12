from __future__ import print_function

import argparse
import logging
import re
import os
import sys

import leip
from mad2.util import  get_filenames, get_all_mad_files

lg = logging.getLogger(__name__)
#lg.setLevel(logging.DEBUG)

@leip.arg('file', nargs='*')
@leip.arg('--dry', help = 'dry run', action='store_true')
@leip.command
def catchup(app, args):
    """
    execute all deferred commands
    """
    for madfile in get_all_mad_files(app, args):

        if madfile.mad.execute.queue:
            queue = madfile.execute.queue
            if not args.dry:
                del(madfile.execute.queue)
                madfile.save()

            executed = []
            for command in queue:
                if args.dry:
                    print(command)
                else:
                    cl = execute(app, madfile, command)
                    executed.append(cl)

            madfile.load()
            if not madfile.execute.history:
                madfile.execute.history = []
            madfile.execute.history.extend(executed)
            madfile.save()


def execute(app, madfile, cl, dry=False):
    """
    execute a command line in the context of this object

    :param dry: do a dry run
    :type dry: boolean
    """
    rendered = madfile.render(cl, app.conf)
    lg.info("executing: {0}".format(rendered))
    if dry:
        print(rendered)
    else:
        os.system(rendered)

    return rendered

def update_history(madfile, executed):
    """
    Add a list of commands to the history

    :param executed: a list of executed commands
    """
    madfile.load()
    if not madfile.execute.history:
        madfile.execute.history = []
    madfile.execute.history.extend(executed)
    madfile.save()


@leip.arg('file', nargs='*')
@leip.arg('comm', metavar='command', help='command to execute (use quotes!)')
@leip.arg('-s', '--save', action='store_true', help='save for later execution')
@leip.arg('--dry', help='dry run', action='store_true')
@leip.command
def x(app, args):
    """
    Execute a command
    """
    command = args.comm
    lg.info("command to execute: {0}".format(command))

    for madfile in get_all_mad_files(app, args):
        if args.save:
            if not madfile.mad.execute.queue:
                madfile.mad.execute.queue = []
            madfile.mad.execute.queue.append(command)
            madfile.save()
        else:
            cl = execute(app, madfile, command, dry=args.dry)
