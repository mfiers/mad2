#!/usr/bin/env python


import logging
import os
import sys
import socket


logging.basicConfig()

lg = logging.getLogger(__name__)
lg.setLevel(logging.DEBUG)


if len(sys.argv) > 1:
    start = sys.argv[1]
else:
    start = '.'

start = os.path.fullpath(start)

lg.info("start dir: {}".format(start))


base = dict(
    socket=socket.gethostname()
)


def process_dir(dir):
    lg.info("processing: {}".format(dir))

    #if (dir / 'mad.ignore')
    for file in dir.files():
        if file.basename()[0] == '.':
            continue
        yield file


    for sub in dir.dirs():
        if sub.basename()[0] == '.':
            continue
        for file in process_dir(sub):
            yield file

for dirpath, dirnames, filenames in os.walk(start):
    print(dirpath)





    #
    #
    # current = Path(currentpath)
    #
    # to_remove = []
    # for d in dirs:
    #     if d[0] == '.':
    #         to_remove.append(d)
    #     if os.path.exists(os.path.join(currentpath, d, 'mad.ignore')):
    #         to_remove.append(d)
    #
    # for d in to_remove:
    #     dirs.remove(d)
    #
    # for f in files:
    #     if f[0] == '.':
    #         continue
    #
    #     filesize =
    #
    #
    # if dirs:
    #     print(dirs)




