
import argparse
import collections
import logging
from multiprocessing import Pool, Lock, Manager
from multiprocessing.dummy import Pool as ThreadPool
import os
import sys
import time

import yaml


from mad2 import hash
from mad2 import util

from colorlog import ColoredFormatter
color_formatter = ColoredFormatter(
    "%(log_color)s%(name)s:%(reset)s "+
    "%(blue)s%(message)s %(purple)s(%(threadName)s)",
    datefmt=None, reset=True,
    log_colors={'DEBUG':    'cyan',
                'INFO':     'green',
                'WARNING':  'yellow',
                'ERROR':    'red',
               'CRITICAL': 'red'})

lg = logging.getLogger('sha1p')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(color_formatter)
logging.getLogger("").handlers = [] #leip defined handlers
lg.setLevel(logging.INFO)
lg.addHandler(stream_handler)

lg.info('start sha1p')


SHADATA = collections.defaultdict(list)
LOCKED = ""
JOBS_DONE = 0
SCHEDULED = 0

def write_both_checksums(dirname, files):
    lg.debug("writing sha1 checksum")
    write_to_checksum_file(
        os.path.join(dirname, 'SHA1SUMS'),
        [[x[0], x[1]] for x in files]
        )
    lg.debug("writing qd checksum")
    write_to_checksum_file(
        os.path.join(dirname, 'QDSUMS'),
        [[x[0], x[2]] for x in files]
        )

def write_to_checksum_file(hashfile, files):

    j = 0
    lg.debug("writing %d sha1sums to %s", len(files), hashfile)

    hashes = {}

    #read old sha1file
    if os.path.exists(hashfile):
        with open(hashfile) as F:
            for line in F:
                hsh, fn = line.strip().split(None, 1)
                hashes[fn] = hsh

    lg.debug("found %d hashes", len(hashes))

    #insert our sha1 - possibly overwriting other version
    for fn, hs in files:
        j += 1
        hashes[fn] = hs

    #write new sha1file
    lg.debug("now has %d hashes", len(hashes))

    try:
        lg.debug("start writing %d hashs to %s", len(hashes), hashfile)
        with open(hashfile, 'w') as F:
            for fn in sorted(hashes.keys()):
                if fn in ['QDSUMS', 'SHA1SUMS']:
                    continue
                F.write("{}  {}\n".format(hashes[fn], fn))

    except IOError:
        lg.warning("can not write to checksum file: %s", hashfile)
        return

    #fix permissions  - but only when root
    if os.geteuid() != 0:
        return j

    #change SHA1SUM file
    dirname = os.path.dirname(hashfile)
    if not dirname.strip():
        dirname = '.'
    dstats = os.stat(dirname)

    if os.path.exists(hashfile):
        os.chmod(hashfile, 0o664)
        os.chown(hashfile, dstats.st_uid, dstats.st_gid)

    return j



def process_file(*args, **kwargs):
    global JOBS_DONE
    try:
        process_file_2(*args, **kwargs)
        JOBS_DONE += 1
    except:
        import traceback
        traceback.print_exception()
        exit(-1)

def process_file_2(datalock, i, fn, force, echo):

    global SHADATA, LOCKED, JOBS_DONE, SCHEDULED

    filename = os.path.basename(fn)
    dirname = os.path.dirname(fn)

    qd_hash_file = os.path.join(dirname, 'QDSUMS')
    sha1_hash_file = os.path.join(dirname, 'SHA1SUMS')

    sha1_file = hash.check_hashfile(sha1_hash_file, filename)

    qd = hash.get_qdhash(fn)
    qd_file = hash.check_hashfile(qd_hash_file, filename)

    if (not force) and (not sha1_file is None) and (qd == qd_file):
        #nothing changed - sha1 is present - not force - return
        return

    #if not in the shasum files - then - maybe there is an old .mad file
    #
    sha1 = None

    madfile = os.path.join(dirname, '.{}.mad'.format(filename))
    if os.path.exists(madfile):
        with open(madfile) as F:
            maf = yaml.load(F)
            if 'hash' in maf:
                sha1 = maf['hash'].get('sha1')
                qd_file = maf['hash'].get('qdhash')

    if qd_file == qd and (not sha1 is None):
        #use mad file sha1
        lg.debug('reusing .mad hash: %s is %s', fn, sha1)
    else:

        sha1 = hash.get_sha1sum(fn)
        lg.debug('calculated hash: %s is %s', fn, sha1)

    datalock.acquire() #processing the SHADATA global data structure - lock
    assert(LOCKED == "")
    LOCKED = "YES"

    #lg.debug("updating SHADATA")

    SHADATA[dirname].append((filename, sha1, qd))

    if i > 0 and i % 100 == 0:
        for dirname in SHADATA:
            if len(SHADATA[dirname]) == 0:
                continue
            #lg.debug('flushing to dir: "%s"', dirname)
            write_both_checksums(dirname, SHADATA[dirname])
            SHADATA[dirname] = []
        lg.info('processed & written %d files (%d scheduled)',
                i, SCHEDULED)

    LOCKED = ""
    datalock.release()


def dispatch():

    global SCHEDULED

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--force', action='store_true')
    parser.add_argument('-d', '--do_dot_dirs', action='store_true')
    parser.add_argument('-j', '--threads', type=int, default=4)
    parser.add_argument('-e', '--echo', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-s', '--silent', action='store_true')
    parser.add_argument('file', nargs='*')

    args = parser.parse_args()

    if args.verbose:
        lg.setLevel(logging.DEBUG)
    if args.silent:
        lg.setLevel(logging.WARNING)

    pool = ThreadPool(args.threads)
    dlock = Lock()
    i = 0

    for i, fn in enumerate(util.get_filenames(args)):
        if '/.' in fn and (not args.do_dot_dirs):
            #no dot dirs
            lg.debug("ignoring in dotdir %s", fn)
            continue
        pool.apply_async(process_file, (dlock, i, fn, args.force, args.echo))
        SCHEDULED += 1

    lg.info(("processed all (%d) files - waiting for threads to " +
            "finish"),i)

    pool.close()

    pool.join()

    lg.info("finished - flushing cache")
    for dirname in SHADATA:
        lg.debug("flushing to %s/SHA1SUMS", dirname)
        if len(SHADATA[dirname]) == 0:
                continue
        write_both_checksums(dirname, SHADATA[dirname])
