from __future__ import print_function

import argparse
import logging
import re
import sys

import leip
from mad2.util import  get_filenames, get_all_mad_files

lg = logging.getLogger(__name__)


@leip.arg('file', nargs='*')
@leip.command
def catchup(app, args):
    """
    execute all deferred commands
    """
    for madfile in get_all_mad_files(app, args):
        madfile.catchup()


def execute(base, madfile, cl, dry=False):
    """
    execute a command line in the context of this object

    :param dry: do a dry run
    :type dry: boolean
    """

    from jinja2 import Template
    import copy

    data = copy.copy(base)
    data.update(copy.copy(madfile.mad.get_data()))
    data['madname'] = self.madname
    data['filename'] = self.filename

    template = Template(cl)
    rendered = template.render(data)

    if dry:
        lg.warning("Executing: {}".format(rendered))
    else:
        lg.warning("Executing: {}".format(rendered))
        os.system(rendered)


@leip.arg('comm', metavar='commands', nargs = argparse.REMAINDER)
@leip.arg('-d', '--defer', action='store_true')
@leip.arg('--dry', help = 'dry run', action='store_true')
@leip.command
def x(app, args):
    """
    Execute a command
    """
    command = " ".join(args.comm)
    lg.info("command to execute: {0}".format(command))

    for madfile in get_all_mad_files(app, args):
        if args.defer:
            if not madfile.mad.execute.queue:
                madfile.mad.execute.queue = []
            madfile.execute.queue.append(" ".join(command))
        else:
            execute(app, madfile, cl, dry=args.dry)

    


            
