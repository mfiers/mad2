from __future__ import print_function

import datetime
import logging
import os

import leip

import mad2.util
from mad2.util import get_all_mad_files, humansize

lg = logging.getLogger(__name__)

#
# @leip.arg('-c', '--command_line')
# @leip.arg('-f', '--derived_from', action='append', default=[])
# @leip.arg('-u', '--used', action='append', default=[])
# @leip.arg('-g', '--generated_by', action='append', nargs=3, default=[],
#           help="name version path")

@leip.arg('file', nargs="*")
@leip.command
def raw(app, args):
    """
    Raw dump of data associated with these files
    """
    for madfile in get_all_mad_files(app, args):
        print(madfile.pretty())


@leip.arg('file', nargs="*")
@leip.command
def prov(app, args):
    """
    Show provenance data
    """
    for madfile in get_all_mad_files(app, args):
        if not 'provenance' in madfile:
            #nothing to show - continue
            continue

        prov_data = madfile['provenance']
        prov_keys = sorted(prov_data.keys())
        latest_key = prov_keys[-1]
        prov = prov_data[latest_key]
        print("provenance_key: {}".format(latest_key))
        print(prov.pretty())


#
#         if args.command_line:
#             madfile.mad['provenance.command_line'] = args.command_line
#
#         derf = madfile.mad['provenance.derived_from']
#         for derived_from in args.derived_from:
#             df_mad = mad2.util.get_mad_file(app, derived_from)
#             df_name = df_mad['filename']
#             dfid = df_mad['qid']
#             derf['{}.path'.format(dfid)] = df_mad['fullpath']
#             derf['{}.host'.format(dfid)] = df_mad['host']
#             if 'hash.sha1' in df_mad:
#                 derf['{}.sha1'.format(dfid)] = df_mad['hash.sha1']
#
#         if args.used:
#             used = madfile.mad['provenance.used']
#         for use in args.used:
#             use_mad = mad2.util.get_mad_file(app, use)
#             use_name = use_mad['filename']
#             usid = use_mad['qid']
#             used['{}.path'.format(usid)] = use_mad['fullpath']
#             if 'hash.sha1' in use_mad:
#                 derf['{}.sha1'.format(dfid)] = use_mad['hash.sha1']
#
#         for i, gb in enumerate(args.generated_by):
#             gb_name, gb_path, gb_version = gb
#             gby = madfile.mad['provenance.generated_by.' + gb_name]
#             gby['path'] = gb_path
#             gby['version'] = gb_version
#         madfile.save()
