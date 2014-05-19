

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
                    hsh, fn = line.strip().split()
                    hashes[fn] = hsh

        #insert our sha1 - possibly overwriting other version
        hashes[filename] = hash

        #write new sha1file
        hashes.keys
        with open(hashfile, 'w') as F:
            for fn in sorted(hashes.keys()):
                F.write("{}  {}\n".format(hashes[fn], fn))


def check_hashfile(hashfile, filename):
    """
    Check a hashfile & return the checksum
    """
    if not os.path.exists(hashfile):
        return None
    with open(hashfile) as F:
        for line in F:
            hsh, fn = line.strip().split()
            if fn == filename:
                return hsh
    return None


def get_sha1sum(filename):
    """
    Calculate the sha1sum

    """
    h = hashlib.sha1()

    blocksize = 2 ** 20
    with open(filename, 'rb') as F:
        for chunk in iter(lambda: F.read(blocksize), b''):
            h.update(chunk)
    return h.hexdigest()


def get_qdhash(filename):
    """
    Provde a quick & dirty hash -

    by no means secure - but a chance of a collision is small.

    .. and this is reasonably fast.
    """
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
