from __future__ import print_function

import logging
import sys

import os
import leip
import hashlib
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)


def hashit(hasher, filename):
    """
    Provde a quick & dirty hash

    this is by no means secure, but quick for very large files, and as long
    as one does not try to create duplicate hashes, the chance is still very
    slim that a duplicate will arise
    """
    h = hasher()
    with open(filename) as F:
        h.update(F.read())
    return h.hexdigest()


@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('file', nargs='*')
@leip.command
def md5(app, args):
    """
    Calculate a checksum
    """
    for madfile in get_all_mad_files(app, args):
        if not args.force and 'md5' in madfile.mad:
            #exists - and not forcing
            lg.warning("Skipping md5 checksum - exists")
            continue
        cs = hashit(hashlib.md5, madfile.filename)
        madfile.mad.hash.md5 = cs
        madfile.save()

@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('file', nargs='*')
@leip.command
def sha1(app, args):
    """
    Calculate a sha1 checksum
    """
    for madfile in get_all_mad_files(app, args):
        if not args.force and 'sha1' in madfile.mad:
            #exists - and not forcing
            lg.warning("Skipping sha1 checksum - exists")
            continue
        cs = hashit(hashlib.sha1, madfile.filename)
        madfile.mad.hash.sha1 = cs
        madfile.save()

# @leip.arg('file', nargs='*')
# @leip.command
# def init(app, args):
#     """
#     Initialize a file, based on extension
#     """

#     exti = app.conf.plugin.init.ext

#     for madfile in get_all_mad_files(app, args):
#         for ext in exti:
#             extdot = '.' + ext
#             if madfile.filename[-1 * len(extdot):] == extdot:
#                 madfile.mad.update(exti[ext])
#         madfile.save()

# @leip.arg('file', nargs='*')
# @leip.arg('category', help='category to assign to these files')
# @leip.commandName('=')
# def apply_category(app, args):
#     """
#     apply a predefined category to a (set of) file(s)
#     """
#     if not args.category in app.conf.template:
#         print("Invalid category: {0}".format(args.category))
#         print("Choose from:")
#         for cat in app.conf.template:
#             print(" - {0}".format(cat))
#         sys.exit()
#     template_data = app.conf.template[args.category]
#     for madfile in get_all_mad_files(app, args):
#         madfile.mad.soft_update(template_data)
#         madfile.save()
