

import collections
import logging
import sys

import fantail
import leip

from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)

def _getter(o, k):
    assert(isinstance(o, fantail.Fantail))
    if '.' in k:
        k1, k2 = k.split('.', 1)
        return _getter(o[k1], k2)
    else:
        return o[k]


@leip.arg('-s', '--sep',  help='separator', default='\t')
@leip.arg('key', help='keys to display', nargs='+')
@leip.command
def table(app, args):
    """
    Create a table
    """
    print('#' + args.sep.join(map(str, args.key)))
    for madfile in get_all_mad_files(app, args):
        values = []
        for k in args.key:
            vv = _getter(madfile.mad, k)
            if str(vv) == '{}':
                vv = _getter(madfile.all, k)
            if str(vv) == '{}':
                vv = ""
            values.append(vv)

        print(args.sep.join(map(str, values)))


@leip.arg('-s', '--sep',  help='separator', default='\t')
@leip.arg('formula', help='groups = numeric variables', nargs='+')
@leip.command
def groupby(app, args):
    """
    Create a table
    """
    #print(args.formula)
    if not '-' in args.formula:
        print("Need to provide a formula, for example:")
        print("mad groupby project username - filesize")
        sys.exit(-1)

    groups = args.formula[:args.formula.index('-')]
    numers = args.formula[args.formula.index('-')+1:]

    print("#" + args.sep.join(groups + numers))
    def empty_numer():
        return [0] * len(numers)

    data = collections.defaultdict(empty_numer)

    for madfile in get_all_mad_files(app, args):
        gv = []
        nm = []

        for g in groups:
            vv = _getter(madfile.mad, g)
            if str(vv) == '{}':
                vv = _getter(madfile.all, g)
            if str(vv) == '{}':
                vv = 'n.d.'
            gv.append(vv)

        for n in numers:
            vv = _getter(madfile.mad, n)
            if str(vv) == '{}':
                vv = _getter(madfile.all, n)

            if str(vv) == '{}':
                vv = 0
            else: vv = int(vv)
            nm.append(vv)

        for i, n in enumerate(nm):
            data[tuple(gv)][i] += n

        #print(gv, nm, data[tuple(gv)][0] / 1000.)

    for k in sorted(data.keys()):
        rv = list(map(str, list(k)))
        rv.extend(list(map(str, data[k])))
        print(args.sep.join(rv))

