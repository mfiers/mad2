

import logging
import os
import socket
import textwrap

import leip

from mad2.util import get_all_mad_files, get_mad_file
from termcolor import cprint

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


@leip.flag('-r', '--raw', help='raw output of provenance data')
@leip.arg('file', nargs="*")
@leip.command
def prov(app, args):
    """
    Show provenance data
    """
    for madfile in get_all_mad_files(app, args):
        if not 'provenance' in madfile:
            # nothing to show - continue
            continue

        prov_data = madfile['provenance']
        prov_keys = sorted(prov_data.keys())
        latest_key = prov_keys[-1]
        prov = prov_data[latest_key]

        if args.raw:
            print("provenance_key: {}".format(latest_key))
            print(prov.pretty())
            return

        def ccp(*args, **kwargs):
            if not 'end' in kwargs:
                kwargs['end'] = ''
            cprint(*args, **kwargs)

        def cckv(key, val, **kwargs):
            cprint(key, "yellow", end=": ")
            cprint(val)

        # pretty output
        cckv("Date", prov['stopped_at_time'])
        cckv("Tool", prov['tool_name'])
        version = prov['tool_version']
        if len(version) > 50:
            ccp('Version: ', "yellow")
            for i, line in enumerate(textwrap.wrap(version,
                                                   initial_indent="         ",
                                                   subsequent_indent="     ")):
                if i == 0:
                    line = line.strip()
                print(line)
        else:
            cckv("Version", prov['tool_version'])

        ccp("Command line:", "yellow", end="\n")
        print(" \\\n".join(textwrap.wrap(prov['kea_command_line'],
                                         initial_indent='  ',
                                         subsequent_indent='       ')))
        cprint("Related files:", "yellow")
        this_host = socket.gethostname()

        for filename in prov['derived_from']:
            finf = prov['derived_from'][filename]
            ccp("  " + finf['category'], 'magenta')
            ccp("/" + filename, "blue")
            ccp("\n")
            ccp("    Host: ", "yellow")
            if finf['host'] == this_host:
                ccp("{host}\n".format(**finf), "green")
            else:
                ccp("{host}\n".format(**finf), "red")

            ccp("    Path: ", "yellow")
            if finf['host'] == this_host:
                if os.path.exists(finf['filename']):
                    ccp("{filename}\n".format(**finf), "green")
                else:
                    ccp("{filename}\n".format(**finf), "red")
            else:
                ccp("{filename}\n".format(**finf), "grey")


            ccp("    Sha1sum: ", "yellow")
            if finf['host'] == this_host:
                fmaf = get_mad_file(app, finf['filename'])
                if fmaf['sha1sum'] == finf['sha1sum']:
                    ccp("{sha1sum}\n".format(**finf), "green")
                else:
                    ccp("{sha1sum}\n".format(**finf), "red")
            else:
                ccp("{sha1sum}\n".format(**finf), "grey")

#        print(finf.pretty())#

