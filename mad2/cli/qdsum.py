
import os
import sys

from mad2 import hash

def dispatch():

    if sys.argv[1] == '-c':
        sumfile = sys.argv[2]
        with open(sumfile) as F:
            for line in F:
                qd, fn = line.strip().split(None, 1)
                fn = fn.strip()
                if not os.path.exists(fn):
                    print("{}: FAILED no such file".format(fn))
                    continue

                qdnew = hash.get_qdhash(fn)
                if qd == qdnew:
                    print("{}: OK".format(fn))
                else:
                    print("{}: FAILED".format(fn))
        exit(0)

    for fn in sys.argv[1:]:
        if os.path.isdir(fn):
            sys.stderr.write("qdsum: {}: Is a directory\n".format(fn))
            continue
        if not os.path.exists(fn):
            sys.stderr.write("qdsum: {}: Does not exist\n".format(fn))
            continue

        qd = hash.get_qdhash(fn)
        print("{}  {}".format(qd, fn))

