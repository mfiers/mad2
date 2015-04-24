import logging
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import os
import leip


from mad2.util import get_all_mad_files
import mad2.hash

lg = logging.getLogger(__name__)
lg.setLevel(logging.DEBUG)


def calc_madfile_sum(madfile, force=False, echo=False,
                     echo_changed=False):

    lg.warning("DEPRECATED")

    if madfile['filename'] in ['SHA1SUMS', 'QDSUMS']:
        return

    if madfile.get('orphan', False):
        # won't deal with orphaned files
        return

    if madfile.get('isdir', False):
        # won't deal with dirs
        return

    if not (madfile.get('qdhash_changed') or force):
        #probably not changed - ignore
        if echo:
            print((madfile['inputfile']))
            return

    dirname = madfile['dirname']
    filename = madfile['filename']

    lg.debug("creating sha1 for %s", filename)

    sha1file = os.path.join(dirname, 'SHA1SUMS')
    qdhashfile = os.path.join(dirname, 'QDSUMS')

    sha1 = mad2.hash.get_sha1sum(os.path.join(dirname, filename))
    mad2.hash.append_hashfile(sha1file, filename, sha1)

    qd = mad2.hash.get_qdhash(madfile['fullpath'])
    mad2.hash.append_hashfile(qdhashfile, filename, qd)

    if echo or echo_changed:
        print(madfile['inputfile'])


# @leip.flag('-f', '--force', help='force recalculation')
# @leip.flag('-E', '--echo_changed', help='echo names of recalculated files')
# @leip.flag('-e', '--echo', help='echo all filenames')
# @leip.arg('file', nargs='*')
# @leip.command
# def sha1(app, args):
#     """
#     Echo the filename

#     note - this ensures that the sha1sum is calculated
#     """
#     lg.warning("DEPRECATED")

#     for madfile in get_all_mad_files(app, args):

#         calc_madfile_sum(madfile, args.force, args.echo, args.echo_changed)


