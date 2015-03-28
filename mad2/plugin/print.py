

import logging

import leip
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)


@leip.arg('file', nargs='*')
@leip.arg('key', help='key to filter on')
@leip.commandName('print')
def mad_print(app, args):
    """
    print a single value from a single file
    """
    for madfile in get_all_mad_files(app, args):
        print(madfile.render(madfile[args.key], [madfile, app.conf]))

