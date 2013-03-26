from __future__ import print_function

import logging

import leip
from mad2.util import get_mad_file

lg = logging.getLogger(__name__)

@leip.arg('file')
@leip.arg('key', help='key to filter on')
@leip.commandName('print')
def mad_print(app, args):
    """
    print a single value from a single file
    """
    madfile = get_mad_file(app, args.file)
    print(madfile.render(madfile.mad[args.key], app.conf))

