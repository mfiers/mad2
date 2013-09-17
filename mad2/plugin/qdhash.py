from __future__ import print_function

import hashlib
import logging
import leip
import os
import sys

lg = logging.getLogger(__name__)

def get_qdhash(filename):
    """
    Provde a quick & dirty hash - a good indication that a file MIGHT have
    changed - but by no means secure.

    It is quick, though.
    """
    sha1sum = hashlib.sha1()
    filesize = os.stat(filename).st_size
    if filesize < 5000:
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


@leip.hook("madfile_load", 150)
def qdhash(app, madfile):
    """
    Calculate a sha1 checksum
    """
    cs = get_qdhash(madfile.filename)
    qdh = madfile.mad.hash.qdhash

    if madfile.mad.hash.qdhash:
        qdh = madfile.mad.hash.qdhash
        if qdh != cs:
            print("{} has changed!".format(madfile.filename),
                file=sys.stderr)

    madfile.mad.hash.qdhash = cs
    madfile.save()
