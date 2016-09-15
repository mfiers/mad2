import logging

import leip
import yaml


import mad2.util
from mad2.util import get_all_mad_files, get_mad_dummy
import mad2.hash

lg = logging.getLogger(__name__)


# make sure stores are cleaned up.
# note that initialization takes place in mad2.util - and only
# when a record is actually saved.
@leip.hook('finish')
def cleanup_stores(app):
    lg.debug("cleanup stores")
    mad2.util.cleanup_stores(app)


@leip.arg('-p', '--prefix', help='prefix keys with this value')
@leip.arg('file', nargs='*')
@leip.arg('table')
@leip.command
def apply_from_table(app, args):
    """apply key/values from a tsv file'

    expects a tsv table. First column is the full file name, each column
    contains metadata (key == column header)
    """

    import pandas as pd
    import numpy as np
    import sys

    if args.table == '-':
        tbl = pd.read_csv(sys.stdin, sep="\t",
                          header=None)
    else:
        tbl = pd.read_csv(args.table, sep="\t")

    if args.prefix:
        tbl[0] = args.prefix + tbl[0]

    tbl.set_index(0, inplace=True)

    outdict = {}

    for k, v in dict(tbl[1]).items():
        if v == 0:
            outdict[k] = 0
        elif isinstance(v, int):
            outdict[k] = int(v)
        elif isinstance(v, np.int64):
            outdict[k] = int(v)
        elif isinstance(v, float):
            outdict[k] = float(v)
        else:
            outdict[k] = v

    for madfile in get_all_mad_files(app, args):
        madfile.update(outdict)
        madfile.save()


@leip.command
def version(app, args):
    import pkg_resources
    print(pkg_resources.get_distribution("mad2").version)


@leip.arg('file', nargs='*')
@leip.command
def echo(app, args):
    """
    Echo the filename

    note - this ensures that the sha1sum is calculated
    """
    for madfile in get_all_mad_files(app, args):
        print((madfile['inputfile']))


@leip.arg('-p', '--progress', action='store_true',
          help='show indication of progress')
@leip.flag('-e', '--echo')
@leip.arg('file', nargs='*')
@leip.command
def save(app, args):
    """
    save the file to the mad datastore

    note - this ensures that the sha1sum is calculated
    """
    lg.debug("start mad save")

    app.trans['progress.save'] = 0

    counter = 0
    for madfile in get_all_mad_files(app, args):
        lg.debug("processing %s", madfile['fullpath'])

        # if madfile['orphan']:
        #    lg.warning("removing %s", madfile['inputfile'])
        #    lg.warning("sha1sum is/was: %s", madfile['sha1sum'])

        counter += 1

        app.trans['progress.save'] += 1
        pp = app.trans['progress.save']
        if args.progress and pp > 0 and pp % 2500 == 0:
            lg.warning("mad save: saved {} files".format(pp))

        madfile.save()
        if args.echo:
            print(madfile['inputfile'])


def _save_dumped_doc(app, doc):
    dm = get_mad_dummy(app, doc)
    dm.save()


@leip.arg('dump_file', help='yaml dump file to load')
@leip.command
def load_dump(app, args):
    with open(args.dump_file) as F:
        for doc in yaml.load_all(F):
            _save_dumped_doc(app, doc)


@leip.arg('-o', '--output_file', help='outputf file to dump to',
          default='dump.yaml')
@leip.arg('file', nargs='*')
@leip.command
def dump(app, args):
    """
    dump a yaml representation of all files.
    """
    F = open(args.output_file, 'w')

    lg.debug("start mad dump")
    i = 0
    for madfile in get_all_mad_files(app, args):
        lg.debug("processing %s", madfile['fullpath'])

        if madfile['orphan']:
            continue

        if i > 0:
            F.write('---\n')

        F.write(madfile.pretty())
        i += 1

    F.close()
