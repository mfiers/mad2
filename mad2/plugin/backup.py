

import logging

import leip
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)


@leip.arg('file', nargs='*')
@leip.command
def backup(app, args):
    """
    Shortcut to mark a file for backup
    """
    for madfile in get_all_mad_files(app, args):
        madfile.mad.backup = True
        madfile.save()

