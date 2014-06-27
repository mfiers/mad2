

import os
import hashlib
import uuid

from lockfile import FileLock


def append_hashfile(hashfile, filename, hash):
    hashes = {}

    with FileLock(hashfile):
        #read old sha1file
        if os.path.exists(hashfile):
            with open(hashfile) as F:
                for line in F:
                    hsh, fn = line.strip().split(None, 1)
                    hashes[fn] = hsh

        #insert our sha1 - possibly overwriting other version
        hashes[filename] = hash

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


def check_hashfile(hashfile, filename):
    """
    Check a hashfile & return the checksum
    """
    if not os.path.exists(hashfile):
        return None
    with open(hashfile) as F:
        for line in F:
            hsh, fn = line.strip().split(None, 1)
            if fn == filename:
                return hsh
    return None


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


def get_qdhash(filename):
    """
    Provde a quick & dirty hash -

    by no means secure - but if used with care, then I'm
    reasaonbly sure that the chance of a collision is small.

    .. and it is fairly fast

    """

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

