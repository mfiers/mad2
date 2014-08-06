
import arrow
import os
import hashlib
import logging
import uuid

from lockfile import FileLock

import mad2.util

lg = logging.getLogger(__name__)


# def sha1sum_check_meta():
#     pass

SHA1CACHE = {}

def get_sha1sum_mad(madfile):
    """
    One function to rule them all!
    """

    global SHA1CACHE

    now_time = str(arrow.get(madfile['mtime']).to('local'))
    now_size = madfile['filesize']

    dirname = madfile['dirname']
    filename = madfile['filename']
    fullpath = madfile['fullpath']

    sha1file = os.path.join(dirname, 'SHA1SUMS')
    qdfile = os.path.join(dirname, 'QDSUMS')

    metafile = os.path.join(dirname, 'SHA1SUMS.META')

    #first check metafile
    meta_time, meta_size = None, None
    if os.path.exists(metafile):
        with open(metafile) as F:
            for line in F:
                meta_time, meta_size, meta_file = line.strip().split(None, 2)
                if meta_file == filename:
                    break
            else:
                meta_time, meta_size = None, None

    stored_sha1 = check_hashfile(sha1file, filename)

    #print(now_time, meta_time)
    if (not stored_sha1 is None) \
            and now_time == meta_time \
            and now_size == int(meta_size):

        #all is well - metadata has not changed - return sha1
        madfile.all['sha1sum'] = stored_sha1
        return

    #print (meta_time, meta_size)

    if  (not stored_sha1 is None) \
            and  (meta_time is None) \
            and (meta_size is None):

        # no metadata - check if there is a qd hash
        # For the time being - see if the QDHASH is correct - until all
        # hash files have a meta file
        now_qd = get_qdhash(fullpath)
        file_qd = check_hashfile(qdfile, filename)
        if now_qd == file_qd:

            #assume all is well.. (re-)store the sha1
            #so that the metadata gets stored as well.
            #sha1 = check_hashfile(sha1file, filename)

            new_store_sha1(sha1file, metafile, filename,
                           stored_sha1, now_time, now_size)
            madfile.all['sha1sum'] = stored_sha1
            return

    # we need to (re-)calculate the SHA1SUM
    #lg.warning("C:" + filename)
    if stored_sha1 is None:
        #no sha1 - assuming this is the first time
        lg.warning("Calculating SHA1SUM for %s", madfile['inputfile'])
    else:
        #there is one, but metadata/qdsum does not match - recalc
        lg.warning("!! recalculating SHA1SUM for %s", madfile['inputfile'])

    sha1 = get_sha1sum(fullpath)
    #and save (regardless - if only to update the metadata)
    new_store_sha1(sha1file, metafile, filename,
                   sha1, now_time, now_size)

    madfile.all['sha1sum'] = sha1

    if not stored_sha1 is None and \
            sha1 != stored_sha1:
        lg.warning("File changed: %s", madfile['inputfile'])
        #store old sha1 for posterity
        madfile['past_sha1sums'] = madfile.mad.get('past_sha1sums', []) + \
                                        [stored_sha1]
        madfile.dirty = True
        madfile.save()

def new_store_sha1(sha1file, metafile, filename,
                   sha1, atime, size):

    hashes = {}
    metas = {}


    #TODO: Figure out if I an do this with file locks - so far it keeps
    #       on giving problems - os for now I'm not.
    #       Not using a lock may result in losing the SHA1SUM - but that
    #       should only result in time loss - since data is linked against
    #       the actual sha1sum.

    lg.debug("writing hash to %s", sha1file)
    #with FileLock(sha1file):
     #read old sha1file
    if os.path.exists(sha1file):
        with open(sha1file) as F:
            for line in F:
                _hsh, _fn = line.strip().split(None, 1)
                hashes[_fn] = _hsh

    #read old metafile
    if os.path.exists(metafile):
        with open(metafile) as F:
            for line in F:
                _date, _size, _fn = line.strip().split(None, 2)
                metas[_fn] = (_date, _size)

    #insert our sha1 - possibly overwriting other version
    hashes[filename] = sha1

    #insert our metadata - possible overwriting previous data
    metas[filename] = (atime, size)

    #write new sha1file
    with open(sha1file, 'w') as F:
        for fn in sorted(hashes.keys()):
            if fn in ['QDSUMS', 'SHA1SUMS']:
                continue
            F.write("{}  {}\n".format(hashes[fn], fn))

    #write new metafile
    with open(metafile, 'w') as F:
        for fn in sorted(metas.keys()):
            if fn in ['QDSUMS', 'SHA1SUMS']:
                continue
            _date, _size = metas[fn]
            #print ('meta', fn, _date, _size)
            F.write("{}  {}  {}\n".format(_date, _size, fn))

#    lg.debug("finished writing hash to %s", sha1file)

#    sha1lock.release()





def append_hashfile(hashfile, filename, hash, date, size):
    lg.warning("DEPRECATED")
    hashes = {}
    metas = {}

    metafile = hashfile + '.META'

    with FileLock(hashfile):
        with FileLock(metafile):
            #read old sha1file
            if os.path.exists(hashfile):
                with open(hashfile) as F:
                    for line in F:
                        hsh, fn = line.strip().split(None, 1)
                        hashes[fn] = hsh

            #read old metafile
            if os.path.exists(metafile):
                with open(metafile) as F:
                    for line in F:
                        date, size, fn = line.strip().split(None, 2)
                        hashes[fn] = (date, size)

            #insert our sha1 - possibly overwriting other version
            hashes[filename] = hash
            #dates[filename =]

            #write new sha1file
            with open(hashfile, 'w') as F:
                for fn in sorted(hashes.keys()):
                    if fn in ['QDSUMS', 'SHA1SUMS']:
                        continue
                    F.write("{}  {}\n".format(hashes[fn], fn))


def get_or_create_sha1sum(filename):
    """
    Get a sha1sum, if it does not exist.

    Also, if there is a qdsum, and it has changed - force
    recalculation of the sha1sum

    """
    lg.warning("DEPRECATED")
    # import traceback
    # traceback.print_stack()

    dirname, basename = os.path.split(filename)
    sha1file = os.path.join(dirname, 'SHA1SUMS')
    qdsumfile = os.path.join(dirname, 'QDSUMS')

    sha1 = check_hashfile(sha1file, basename)
    qd_old = check_hashfile(qdsumfile, basename)
    qd_now = get_qdhash(filename)

    if (sha1 is None) or (qd_old != qd_now):
        sha1 = get_sha1sum(filename)
        append_hashfile(sha1file, basename, sha1)
        append_hashfile(qdsumfile, basename, qd_now)
    return sha1


HASH_CACHE = {}

def check_hashfile(hashfile, filename):
    """
    Check a hashfile & return the checksum
    """

    global HASH_CACHE

    if not os.path.exists(hashfile):
        return None

    #cached??
    hckey = (hashfile, filename)
    if hckey in HASH_CACHE:
        return HASH_CACHE[hckey]

    with open(hashfile) as F:
        for line in F:
            hsh, fn = line.strip().split(None, 1)
            HASH_CACHE[(hashfile, fn)] = hsh

    return HASH_CACHE.get(hckey)


def get_sha1sum(filename):
    """
    Calculate the sha1sum

    """

    if not os.path.exists(filename):
        #not sure what to do with files that do not exist (yet)
        return None

    h = hashlib.sha1()

    blocksize = 2 ** 20
    with open(filename, 'rb') as F:
        for chunk in iter(lambda: F.read(blocksize), b''):
            h.update(chunk)
    return h.hexdigest()


@mad2.util.memoized
def get_qdhash(filename):
    """
    Provde a quick & dirty hash -

    by no means secure - but if used with care, then I'm
    reasaonbly sure that the chance of a collision is small.

    .. and it is fairly fast

    """
    lg.warning("DEPRECATED get_qdhash")

#    lg.critical("MM")
    if not os.path.exists(filename):
        #not sure what to do with files that do not exist (yet)
        return None

    if os.path.isdir(filename):

        #qdid for directories is a uuid - stored in .mad/qid
        maddir = os.path.join(filename, '.mad')
        if not os.path.exists(maddir):
            os.makedirs(maddir)
        qidfile = os.path.join(maddir, 'qid')
        if not os.path.exists(qidfile):
            u = str(uuid.uuid4()).replace('-', '')[:24]
            with open(qidfile, 'w') as F:
                F.write(u)
            return u
        else:
            with open(qidfile) as F:
                u = F.read().strip()
            return u


    sha1sum = hashlib.sha1()
    filesize = os.stat(filename).st_size
    if filesize < 20000:
        with open(filename, 'rb') as F:
            sha1sum.update(F.read())
    else:
        with open(filename, 'rb') as F:
            for x in range(9):
                F.seek(int(filesize * (x / 10.0)))
                sha1sum.update(F.read(2000))

            F.seek(-2000, 2)
            sha1sum.update(F.read())

#    return sha1sum.hexdigest()[:24]#
    return sha1sum.hexdigest()

