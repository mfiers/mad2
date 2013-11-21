from __future__ import print_function

import datetime
import logging
import os

import leip
from mad2.util import  get_all_mad_files

lg = logging.getLogger(__name__)

def execute(app, madfile, cl, dry=False):
    """
    execute a command line in the context of this object

    :param dry: do a dry run
    :type dry: boolean
    """
    lg.info("executing: {0}".format(cl))
    if dry:
        print(cl)
    else:
        os.system(cl)
        t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        h = madfile.mad.execute.history[t]
        h.cl = cl
        madfile.save()#
#        madfile.execute.history:
#           madfile.execute.history = []           madfile.execute.history.extend(executed)
#     madfile.save()


@leip.arg('file', nargs='*')
@leip.arg('comm', metavar='command', help='predefined command to execute')
@leip.arg('-d', '--dry', help='dry run', action='store_true')
@leip.command
def x(app, args):
    """
    Execute a command
    """
    command = args.comm
    lg.info("command to execute: {0}".format(command))

    for madfile in get_all_mad_files(app, args):
        cl = madfile.render(madfile.x[command], app.conf)
        execute(app, madfile, cl, dry=args.dry)
