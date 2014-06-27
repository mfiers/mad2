import logging
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import os
import leip


from mad2.util import get_all_mad_files
import mad2.hash

lg = logging.getLogger(__name__)
lg.setLevel(logging.DEBUG)


@leip.hook("madfile_post_load", 50)
def sha1hook_new(app, madfile):

    if madfile.get('orphan', False):
        # won't deal with orphaned files
        return
    if madfile.get('isdir', False):
        # won't deal with dirs
        return

    dirname = madfile['dirname']
    filename = madfile['filename']

    sha1file = os.path.join(dirname, 'SHA1SUMS')
    qdhashfile = os.path.join(dirname, 'QDSUMS')

    sha1 = mad2.hash.check_hashfile(sha1file, filename)

    #see if we can get the hash from the old mad file -
    sha1_mad_oldstyle = None
    if 'hash.sha1' in madfile:
        sha1_mad_oldstyle = madfile['hash.sha1']

    if sha1 is None:
        #if not in the hashfile - calculate & add to the hashfile

        if not sha1_mad_oldstyle is None:
            sha1 = sha1_mad_oldstyle
        else:
            sha1 = mad2.hash.get_sha1sum(madfile['fullpath'])

        mad2.hash.append_hashfile(sha1file, filename, sha1)

        qd = mad2.hash.get_qdhash(madfile['fullpath'])
        mad2.hash.append_hashfile(qdhashfile, filename, qd)

    madfile.all['sha1sum'] = sha1


def calc_madfile_sum(madfile, force=False, echo=False,
                     echo_changed=False):

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
            print(madfile['inputfile'])
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
        print madfile['inputfile']


@leip.flag('-f', '--force', help='force recalculation')
@leip.flag('-E', '--echo_changed', help='echo names of recalculated files')
@leip.flag('-e', '--echo', help='echo all filenames')
@leip.arg('file', nargs='*')
@leip.command
def sha1(app, args):
    """
    Echo the filename

    note - this ensures that the sha1sum is calculated
    """
    for madfile in get_all_mad_files(app, args):

        calc_madfile_sum(madfile, args.force, args.echo, args.echo_changed)


@leip.arg('file', nargs='*')
@leip.command
def qdhash(app, args):
    """
    print the qdhash to screen
    """
    for madfile in get_all_mad_files(app, args):
        qd = mad2.hash.get_qdhash(madfile['fullpath'])
        print('{} \t {}'.format(madfile['inputfile'], qd))


@leip.arg('file', nargs='*')
@leip.command
def echo(app, args):
    """
    Echo the filename

    note - this ensures that the sha1sum is calculated
    """
    for madfile in get_all_mad_files(app, args):
        print(madfile['inputfile'])


@leip.flag('-e', '--echo')
@leip.arg('file', nargs='*')
@leip.command
def save(app, args):
    """
    save the file to the mad datastore

    note - this ensures that the sha1sum is calculated
    """
    for madfile in get_all_mad_files(app, args):
        madfile.save()
        if args.echo:
            print madfile['inputfile']

