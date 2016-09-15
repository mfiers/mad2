
import collections
import logging
import os
import sys

import fantail
import leip
import numpy as np

from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)


def _getter(o, k):
    assert(isinstance(o, fantail.Fantail))
    if '.' in k:
        k1, k2 = k.split('.', 1)
        return _getter(o[k1], k2)
    else:
        return o[k]


@leip.flag('-t', '--test')
@leip.arg('file', nargs='*')
@leip.arg('-s', '--sep', help='separator', default='\t')
@leip.arg('-o', '--outfile', help='file to output the table to', default='-')
@leip.arg('-a', '--aggregate_on', help='aggreate on this field')
@leip.command
def table(app, args):
    """
    Create a table
    """

    import pandas as pd
    rv = []

    if args.test and os.path.exists('__test.pickle'):

        d = pd.read_pickle('__test.pickle')
        lg.warning('read test data %s' % str(d.shape))
    else:
        for madfile in get_all_mad_files(app, args):
            rv.append(dict(madfile))

        d = pd.DataFrame(rv)
        if args.test:
            d.to_pickle('__test.pickle')

    allcolls = list(d.columns)
    agg_func = {}

    if not args.aggregate_on:
        if args.outfile == '-':
            d.to_csv(sys.stdout, sep=args.sep)
        else:
            d.to_csv(args.outfile, sep=args.sep)
        return

    d = d.convert_objects(convert_numeric=True)

    for s in '_reads_ no_ _no count _mapped_ pairs _total_'.split():
        for ss in [x for x in allcolls if s in x]:
            agg_func[ss] = np.sum
    for s in 'mean perc _per_ score rate'.split():
        for ss in [x for x in allcolls if s in x]:
            agg_func[ss] = np.mean

    remove_from_agg = []
    for k in agg_func:
        nums = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        if not d[k].dtype in nums:
            remove_from_agg.append(k)

    for k in remove_from_agg:
        del agg_func[k]

    aggcols = [args.aggregate_on] + list(agg_func.keys())

    agg = d[aggcols].groupby(args.aggregate_on).agg(agg_func)

    if args.outfile == '-':
        agg.to_csv(sys.stdout, sep=args.sep)
    else:
        agg.to_csv(args.outfile, sep=args.sep)


@leip.arg('-s', '--sep', help='separator', default='\t')
@leip.arg('formula', help='groups = numeric variables', nargs='+')
@leip.command
def groupby(app, args):
    """
    Create a table
    """
    # print(args.formula)
    if '-' not in args.formula:
        print("Need to provide a formula, for example:")
        print("mad groupby project username - filesize")
        sys.exit(-1)

    groups = args.formula[:args.formula.index('-')]
    numers = args.formula[args.formula.index('-') + 1:]

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
            else:
                vv = int(vv)
            nm.append(vv)

            for i, n in enumerate(nm):
                # TODO: check if this is ok!
                data[tuple(gv)][i] += n

        # print(gv, nm, data[tuple(gv)][0] / 1000.)

    for k in sorted(data.keys()):
        rv = list(map(str, list(k)))
        rv.extend(list(map(str, data[k])))
        print(args.sep.join(rv))
