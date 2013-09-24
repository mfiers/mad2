from __future__ import print_function

import logging
import sys

import leip
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)


@leip.arg('file', nargs='*')
@leip.arg('category', help='category to assign to these files')
@leip.commandName('=')
def apply_category(app, args):
    """
    apply a predefined category to a (set of) file(s)
    """
    if not args.category in app.conf.template:
        print("Invalid category: {0}".format(args.category))
        print("Choose from:")
        for cat in app.conf.template:
            print(" - {0}".format(cat))
        sys.exit()
    template_data = app.conf.template[args.category]
    for madfile in get_all_mad_files(app, args):
        madfile.mad.update(template_data)
        madfile.save()
