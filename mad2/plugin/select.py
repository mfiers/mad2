

import re
import logging
import sys

import leip
from mad2.util import  get_filenames, get_all_mad_files, boolify

lg = logging.getLogger(__name__)

OPERATORS =  {'<' : lambda x,y: x < y,
              '<=' : lambda x,y: x <= y,
              '==' : lambda x,y: x == y,
              '=' : lambda x,y: x == y,
              '' : lambda x,y: x == y,
              '>' : lambda x,y: x > y,
              '>=' : lambda x,y: x > y,
              '!=' : lambda x,y: x != y,
              '!' : lambda x,y: x != y,
              '<>' : lambda x,y: x != y
              }


def is_numeric(val):
    if numre:
        return True
    return False

#@leip.arg('file', nargs='*')
@leip.arg('value', help='value to filter with',
    nargs='?', default=True)
@leip.arg('operator', help='operator (= < > != ! <>)',
    nargs='?', default='=')
@leip.arg('key', help='key to filter on')
@leip.command
def select(app, args):
    """
    Select a set of files based on their metadata

    This file needs to be fed filenames on stdin

    The command can take three forms

        mad find | mad select key operator value

        mad find | mad select key value # operator: =

        mad find | mad select key # only booleans, = True

    """
    val = args.value
    key = args.key
    oper = args.operator
    ofunc = OPERATORS.get(oper, lambda x,y: x==y)

    lg.debug("compare '{0}' '{1}' '{2}'".format(key, oper, val))

    def compare_func(what):
        try:
            if isinstance(what, int):
                return ofunc(what, int(val))
        except:
            pass
        try:
            if isinstance(what, float):
                return ofunc(what, float(val))
        except:
            pass
        try:
            if isinstance(what, bool):
                return ofunc(what, boolify(val))
        except:
            pass

        return ofunc(what, val)

    for madfile in get_all_mad_files(app, args):
        kv = madfile.mad.get(args.key, None)
        if kv is None:
            continue

        if compare_func(kv):
            print(madfile.filename)
#            print(madfile.fullpath)







