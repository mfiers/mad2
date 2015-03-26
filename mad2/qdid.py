

import os
import hashlib
import uuid


def get_qdhash(filename):
    """
    Provde a quick & dirty hash -

    by no means secure - but a chance of a collision is small.

    .. and this is reasonably fast.
    """
    lg.warning("DEPRECATED")
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

    sha1sum = hashlib.sha256()
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

    return sha1sum.hexdigest()[:24]
