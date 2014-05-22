from __future__ import print_function

import datetime
import logging
import sys
import os
import leip
import hashlib

from lockfile import FileLock

from mad2.util import get_all_mad_files
from mad2.hash import get_qdhash

lg = logging.getLogger(__name__)


def check_qdsumfile(qdfile, filename):
    if not os.path.exists(qdfile):
        return None
    with open(qdfile) as F:
        for line in F:
            hsh, fn = line.strip().split()
            if fn == filename:
                return hsh
    return None


def append_qdsumfile(qdfile, filename, qd):
    qds = {}

    with FileLock(qdfile):
        #read old qdfile
        if os.path.exists(qdfile):
            with open(qdfile) as F:
                for line in F:
                    hsh, fn = line.strip().split()
                    qds[fn] = hsh

        #insert our qd - possibly overwriting other version
        qds[filename] = qd

        #write new qdfile
        qds.keys
        with open(qdfile, 'w') as F:
            for fn in sorted(qds.keys()):
                F.write("{}  {}\n".format(qds[fn], fn))


@leip.hook("madfile_post_load", 250)
def qdhook(app, madfile):

    if madfile.get('orphan', False):
        # won't deal with orphaned files
        return
    if madfile.get('isdir', False):
        # won't deal with dirs
        return

    dirname = madfile['dirname']
    filename = madfile['filename']

    qdfile = os.path.join(dirname, 'QDSUMS')
    qd_file = check_qdsumfile(qdfile, filename)
    qd = get_qdhash(madfile['fullpath'])

    if qd_file is None:
        #qd does not exists yet - create & return
        append_qdsumfile(qdfile, filename, qd)
        madfile.all['qdhash'] = qd
        return

    #else - check if the qd has chenged
    if qd_file == qd:
        #no? All is well -
        return

    lg.warning("'%s' may have hanged (qd hash did)", madfile['inputfile'])


#    madfile.all['qdsum'] = qd


# def get_mtime(fn):
#     return datetime.datetime.utcfromtimestamp(
#         os.stat(fn).st_mtime).isoformat()


# def may_have_changed(madfile):
#     may_have_changed = False
#     if madfile.get('hash.qdhash', False):
#         qmt = madfile['hash.mtime']
#         mtime = get_mtime(madfile['fullpath'])
#         if qmt != mtime:
#             may_have_changed = True
#     elif madfile.get('hash.mtime', False):
#         qdh = madfile['hash.qdhash']
#         cs = get_qdhash(madfile['fullpath'])
#         if qdh != cs:
#             may_have_changed = True
#     return may_have_changed


# @leip.hook("madfile_post_load", 250)
# def hashhelper(app, madfile):
#     """
#     Calculate a quick&dirty checksum

#     """
#     if madfile.get('orphan', False):
#         # cannot deal with orphaned files
#         return

#     changed = may_have_changed(madfile)

#     if changed and not 'qd' in sys.argv:
#         print("{} may have changed! (rerun mad qd)".format(
#             madfile['fullpath']), file=sys.stderr)


# def hashit(hasher, filename):
#     """
#     Provde a quick & dirty hash

#     this is by no means secure, but quick for very large files, and as long
#     as one does not try to create duplicate hashes, the chance is still very
#     slim that a duplicate will arise
#     """
#     h = hasher()
#     blocksize = 2 ** 20
#     with open(filename, 'rb') as F:
#         for chunk in iter(lambda: F.read(blocksize), b''):
#             h.update(chunk)
#     return h.hexdigest()


# @leip.arg('-E', '--echo_scanned', action='store_true',
#           help='echo only those checked')
# @leip.arg('-e', '--echo', action='store_true', help='echo name')
# @leip.arg('-c', '--changed', action='store_true', help='echo changed state')
# @leip.arg('-f', '--force', action='store_true', help='apply force')
# @leip.arg('-w', '--warn', action='store_true', help='warn when skipping')
# @leip.arg('file', nargs='*')
# @leip.command
# def qd(app, args):
#     """
#     Calculate a qd checksum
#     """
#     apply_checksum(app, args, 'qd')


# @leip.arg('-E', '--echo_scanned', action='store_true',
#           help='echo only those checked')
# @leip.arg('-e', '--echo', action='store_true', help='echo name')
# @leip.arg('-c', '--changed', action='store_true', help='echo changed state')
# @leip.arg('-f', '--force', action='store_true', help='apply force')
# @leip.arg('-w', '--warn', action='store_true', help='warn when skipping')
# @leip.arg('file', nargs='*')
# @leip.command
# def md5(app, args):
#     """
#     Calculate a md5 checksum
#     """
#     apply_checksum(app, args, 'md5')


# def apply_checksum_madfile(args, madfile, ctype):

#     if madfile.get('orphan', False):
#         return

#     if os.path.isdir(madfile['inputfile']):
#         #is a directory
#         lg.debug("skipping directory %s", madfile)
#         return

#     changed = may_have_changed(madfile)

#     if args.get('echo'):
#         if args.changed:
#             cp = 'u'
#             if changed:
#                 cp = 'c'
#             print('{}\t{}'.format(cp, madfile['inputfile']))
#         else:
#             print(madfile['inputfile'])

#     if not args.get('force'):
#         if madfile.mad.get('hash.{}'.format(ctype)):
#             if not changed:
#                 if args.warn:
#                     # exists - and not forcing
#                     lg.warning(
#                         "Skipping %s checksum - exists & likely unchanged",
#                         ctype)
#                 return

#     lg.debug("calculating %s checksum for %s",
#              ctype, madfile['inputfile'])
#     qd = get_qdhash(madfile['inputfile'])
#     mtime = get_mtime(madfile['inputfile'])

#     madfile.mad['hash.qdhash'] = qd
#     madfile.mad['hash.mtime'] = mtime

#     cs = hashit(hashlib.__dict__[ctype], madfile['inputfile'])
#     madfile.mad['hash.{}'.format(ctype)] = cs

#     madfile.save()

#     if args.get('echo_scanned'):
#         print(madfile['inputfile'])

# def apply_checksum(app, args, ctype='qd'):

#     for madfile in get_all_mad_files(app, args):
#         apply_checksum_madfile(args, madfile, ctype)

