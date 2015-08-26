
import hashlib
import logging
import pytz
import os

import arrow
import iso8601

lg = logging.getLogger(__name__)
# lg.setLevel(logging.DEBUG)


def get_sha1(filename):
    """
    Return the sha1sum for a certain filename - expected is a full path
    """

    if not os.path.exists(filename):
        # not sure what to do with files that do not exist (yet)
        return None

    h = hashlib.sha1()
    blocksize = 2 ** 20

    try:
        with open(filename, 'rb') as F:
            for chunk in iter(lambda: F.read(blocksize), b''):
                h.update(chunk)
        return h.hexdigest()
    except IOError:
        #something went wrong reading the file (no permissions?? ignore)
        return None


SHA1_CACHE = {}
SHA1_TIME_CACHE = {}
def check_sha1sum_file(fullpath):


    global SHA1_CACHE
    lg.debug("checking sha1sum file for %s", fullpath)

    dirname = os.path.dirname(fullpath)
    basename = os.path.basename(fullpath)

    sha1sumfile = os.path.join(dirname, 'SHA1SUMS')
    sha1metafile = os.path.join(dirname, 'SHA1SUMS.META')

    if not os.path.exists(sha1sumfile) \
       or not os.path.exists(sha1metafile):
        return None, None

    sha1 = None
    sha1_time = None

    with open(sha1sumfile) as F:
        for line in F:
            ls = line.strip().split(None, 1)
            if len(ls) != 2: continue
            ssf_fullpath = os.path.join(dirname, ls[1])
            ssf_sha1 = ls[0]

            SHA1_CACHE[ssf_fullpath] = ssf_sha1
            if ls[1] == basename:
                sha1 = ssf_sha1

    if not sha1:
        return None, None

    lg.debug("SHA1SUM file sha1: %s", sha1)
    try:
        with open(sha1metafile) as F:
            for line in F:
                ls = line.strip().split(None, 2)
                smf_fullpath = os.path.join(dirname, ls[2])
                smf_time = iso8601.parse_date(ls[0])
                smf_time = arrow.get(smf_time).to('local')
                #smf_time = pytz.utc.localize(smf_time)
                SHA1_TIME_CACHE[smf_fullpath] = smf_time
                if ls[2] == basename:
                    sha1_time = smf_time.datetime
    except:
        #if anything goes wrong - simple ignore this sha1sum - recalculate
        return None, None

    if sha1_time is None:
        return None, None

    return sha1, sha1_time
