from __future__ import print_function

import datetime
import logging
from multiprocessing.dummy import Pool as ThreadPool
import os
import subprocess as sp

import leip
import Yaco
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)


class SimpleExecutor(object):

    def __init__(self, app, args):
        self.app = app
        self.args = args
        lg.debug("opening thread pool with %d threads",
                args.threads)
        self.pool = ThreadPool(args.threads)

    def execute(self, cl):
        """
        Execute a single command line
        """
        def _applicator(cl, bg):
            if bg:
                sp.Popen(cl, shell=True)
            else:
                sp.call(cl, shell=True)

        cl = cl.strip().rstrip('&')
        rv = Yaco.Yaco()
        rv.cl = cl
        self.pool.apply_async(_applicator, (cl, self.args.bg))
        return rv

    def finish(self):
        """
        Close & join the executor pool
        """
        self.pool.close()
        self.pool.join()
        lg.info("finished executing")


def execute(app, madfile, execinfo, executor, dry=False):
    """
    execute a command line in the context of this object

    :param dry: do a dry run
    :type dry: boolean
    """
    command = execinfo.command
    cl = madfile.render(command, app.conf)

    lg.info("executing: {0}".format(cl))
    if dry:
        print(cl)
    else:
        runinfo = executor.execute(cl)
        t = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        h = madfile.mad.execute.history[t]
        h.update(runinfo)
        process_type = execinfo.get('type', None)
        if process_type == 'transform':
            handle_process_transform(madfile, execinfo)
        madfile.save()


def _get_command(app, madfile, command_name):

        execinfo = madfile.x[command_name]

        if isinstance(execinfo, str):
            # simple command
            rv = Yaco.Yaco()
            rv.command = execinfo
            rv.description = execinfo
            return rv
        else:
            return execinfo


@leip.arg('file', nargs='*')
@leip.command
def commands(app, args):
    """
    Commands available:
    """
    for madfile in get_all_mad_files(app, args):
        print(madfile.filename)
        if not madfile.x:
            print(" -- no commands available")
        else:
            for command_name in madfile.x:
                execinfo = _get_command(app, madfile, command_name)
                print(" {}: {}".format(
                    command_name,
                    execinfo.description))


@leip.arg('file', nargs='*')
@leip.arg('comm', metavar='command', help='predefined command to execute')
@leip.arg('-d', '--dry', help='dry run', action='store_true')
@leip.arg('-j', '--threads', help='no of threads', type=int, default=1)
@leip.arg('--bg', help='run in the background', action='store_true')
@leip.command
def x(app, args):
    """
    Execute a command
    """
    command_name = args.comm

    executor = SimpleExecutor(app, args)
    for madfile in get_all_mad_files(app, args):
        info = _get_command(app, madfile, command_name)
        execute(app, madfile, info, executor)

    executor.finish()
