

import argparse
import logging
import os
import subprocess as sp
import shutil

import leip

from mad2.util import to_mad, get_mad_file

lg = logging.getLogger(__name__)

# @leip.arg('to')
# @leip.arg('fr', metavar='from')
# @leip.command
# def mmv(app, args):
#     """
#     Move a single file and the corresponding mad file.

#     If the file has already been moved - must move the
#     mad file.
#     """
#     fr = args.fr
#     frex = os.path.exists(fr)
#     frmad = get_mad_file(app, fr)
#     frmex = os.path.exists(frmad.madfile)

#     to = args.to
#     toex = os.path.exists(to)
#     tomad = get_mad_file(app, to)
#     tomex = os.path.exists(tomad.madfile)

#     if frex and toex:
#         lg.warning("Can't move %s", fr)
#         lg.warning(" | %s exists", to)
#         exit(-1)
# #    if frex:
# #        shutil.m

#     print("moving {} to {}".format(fr, to))

# #    if frex and not toex:
# #    if os.path.exists(fr) and not os


@leip.arg('args', nargs=argparse.REMAINDER)
@leip.command
def cp(app, args):
    """
    Fake copy - and make sure that the .mad files go along.
    """
    a = list(args.args)
    if not '-v' in a:
        a = ['-v'] + a
    a = ['cp'] + a
    P = sp.Popen(a, stdout=sp.PIPE)
    out, _ = P.communicate()
    froms, tos = [], []
    for line in out.split("\n"):
        line = line.strip()
        if not line: continue
        if line.count(' -> ') != 1:
            lg.warning("invalid cp output: ({}) {}".format(
                line.count(' -> '), line))
            continue
        fr, to = line.split(' -> ')
        froms.append(fr)
        tos.append(to)

    for fr, to in zip(froms, tos):
        frbase = fr.rsplit('/')[-1]
        if frbase[0] == '.' and frbase[-4:] == '.mad':
            #if this is a mad file, ignore
            continue

        frmad = to_mad(fr)
        tomad = to_mad(to)
        if frmad in froms:
            #if the mad file is already copied - don't bother
            continue
        shutil.copy(frmad, tomad)
        lg.debug("Copying mad file {} to {}".format(frmad, tomad))


