from __future__ import print_function

import datetime
import logging
import sys
import os
import leip
import hashlib

from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)

def get_qdhash(filename):
    """
    Provde a quick & dirty hash - a good indication that a file MIGHT have
    changed - but by no means secure.

    It is quick, though.
    """
    sha1sum = hashlib.sha1()
    filesize = os.stat(filename).st_size
    if filesize < 20000:
        with open(filename, 'rb') as F:
            sha1sum.update(F.read())
    else:
        with open(filename, 'rb') as F:
            for x in range(9):
                F.seek(int(filesize * (x/10.0)))
                sha1sum.update(F.read(2000))

            F.seek(-2000, 2)
            sha1sum.update(F.read())

    return sha1sum.hexdigest()


def get_mtime(fn):
    return datetime.datetime.utcfromtimestamp(
        os.stat(fn).st_mtime).isoformat()

@leip.hook("madfile_post_load", 250)
def hashhelper(app, madfile):
    """
    Calculate a quick&dirty checksum

    """

    if madfile.orphan:
        #cannot deal with orphaned files
        return

    may_have_changed = False
    if madfile.mad.hash.qdhash:
        qmt = madfile.mad.hash.mtime
        mtime =get_mtime(madfile.fullpath)
        if qmt != mtime:
            may_have_changed = True
    elif madfile.mad.hash.mtime:
        qdh = madfile.mad.hash.qdhash
        cs = get_qdhash(madfile.fullpath)
        if qdh != cs:
            may_have_changed = True

    if may_have_changed:
        print("{} may have changed! (rerun mad sha1)".format(madfile.fullpath),
              file=sys.stderr)

def hashit(hasher, filename):
    """
    Provde a quick & dirty hash

    this is by no means secure, but quick for very large files, and as long
    as one does not try to create duplicate hashes, the chance is still very
    slim that a duplicate will arise
    """
    h = hasher()
    blocksize = 2**20
    with open(filename, 'rb') as F:
        for chunk in iter(lambda: F.read(blocksize), b''):
            h.update(chunk)
    return h.hexdigest()


@leip.arg('-e', '--echo', action='store_true', help='echo name')
@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('file', nargs='*')
@leip.command
def sha1(app, args):
    """
    Calculate a sha1 checksum
    """

    for madfile in get_all_mad_files(app, args):

        if madfile.all.orphan:
            return

        if not args.force and 'sha1' in madfile.mad:
            #exists - and not forcing
            lg.warning("Skipping sha1 checksum - exists")
            continue

        qd = get_qdhash(madfile.filename)
        mtime =get_mtime(madfile.filename)

        madfile.mad.hash.qdhash = qd
        madfile.mad.hash.mtime = mtime

        cs = hashit(hashlib.sha1, madfile.filename)
        madfile.mad.hash.sha1 = cs

        madfile.save()
        if args.echo:
            print(madfile.filename)


