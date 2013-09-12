from __future__ import print_function

import logging
import sys

import os
import leip
import hashlib
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)


def _get_qdhash(filename):
    """
    Provde a quick & dirty hash

    this is by no means secure, but quick for very large files, and as long
    as one does not try to create duplicate hashes, the chance is still very
    slim that a duplicate will arise
    """
    sha1sum = hashlib.sha1()
    filesize = os.stat(filename).st_size
    if filesize < 10000:
        with open(filename) as F:
            sha1sum.update(F.read().encode())
    else:
        with open(filename) as F:
            sha1sum.update(F.read(2000))
            F.seek(int(filesize * 0.4))
            sha1sum.update(F.read(2000))
            F.seek(-2000, 2)
            sha1sum.update(F.read())
    return sha1sum.hexdigest()

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
        lg.info("Processing %s for md5sum" % madfile.filename)
        cs = hashit(hashlib.md5, madfile.filename)
        madfile.mad.hash.md5 = cs
        madfile.save()
        print(madfile.filename)

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
        print(madfile.filename)

@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('file', nargs='*')
@leip.command
def qdhash(app, args):
    """
    Calculate a sha1 checksum
    """
    for madfile in get_all_mad_files(app, args):
        if not args.force and 'qdhash' in madfile.mad:
            #exists - and not forcing
            lg.warning("Skipping sha1 checksum - exists")
            continue
        cs = _get_qdhash(madfile.filename)
        madfile.mad.hash.qdhash = cs
        madfile.save()
        print(madfile.filename)

