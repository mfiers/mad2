from __future__ import print_function

import re
import logging
import sys

import leip
from mad2.util import  get_filenames, get_all_mad_files

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
    
@leip.arg('file', nargs='*')
@leip.arg('value', help='value to filter with')
@leip.arg('key', help='key to filter on')
@leip.command
def select(app, args):
    """
    Select a set of files based on their metadata
    """
    val = args.value
    lg.debug("compare {0} vs '{1}'".format(args.key, val))

    def compare_func(what):
        return what == val
    
    lg.debug("compare {0} vs '{1}'".format(args.key, val))
    #see if it is a numeric or regex comparison:
    numre = re.match('([><!=]{,2})([0-9\.]{1,})', val)
    if numre:
        operator, number = numre.groups()
        def compare_func(a):
            cf =  OPERATORS.get(operator, lambda x,y: x==y)
            return cf(float(a), float(number))
    elif val[0] == '/' and val[-1] == '/':
        regex = re.compile(val[1:-1])
        def compare_func(a):
            return regex.search(a)

    for madfile in get_all_mad_files(app, args):
        kv = madfile.mad.get(args.key, None)
        if not kv is None and compare_func(kv):
            print(madfile.filename)

                
    
    


            
