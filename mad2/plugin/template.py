from __future__ import print_function

import collections
import logging
from multiprocessing.dummy import Pool as ThreadPool
import os
import subprocess as sp
import sys
import uuid

import leip
import fantail
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)
# lg.setLevel(logging.DEBUG)

@leip.command
def t(app, args):
    """
    Render a template
    """
    command_name = args.comm

    executor = Executor(app, args)

    if args.groupby:
        group_on = args.groupby
        lg.info("Grouping on %s", group_on)
        groups = collections.defaultdict(list)
        for f in get_all_mad_files(app, args):
            groups[f[group_on]].append(f)
        files = groups.values()
    else:
        files = [[x] for x in get_all_mad_files(app, args)]

    print(list(f[0]['filename'] for f in files))

    for madfileset in files:

        #print('xxxxxxx', madfileset[0]['filename'])
        for madfile in madfileset:
            command_info = _get_command(app, madfile, command_name)
            if not command_info:
                lg.error("Command {} does not exists for {}".format(
                         command_name, madfile['filename']))
                exit(-1)

        runinfo = executor.execute(madfileset, command_info)

        # t = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        # h = madfile.mad.execute.history[t]
        # h.update(runinfo)
        # madfile.save()

    executor.finish()
# h()
# madfile.save()

#     executor.finish()
# h()
