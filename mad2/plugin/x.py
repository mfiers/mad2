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

    def __init__(self, app, args, executor_type='simple'):
        self.app = app
        self.args = args
        self.executor_type = executor_type
        confName = executor_type.capitalize() + "Executor"
        self.conf = app.conf.plugin.x[confName]
        self.defaults = app.conf.plugin.x[confName].defaults

        lg.debug("opening thread pool with %d threads",
                args.threads)
        self.pool = ThreadPool(args.threads)


    def prepare_script(self, madfile, command_info):
        """
        prepare command line & full execution prepare_script

        """

        conf_objects = [self.app.conf,
                        command_info.defaults,
                        self.defaults]

        command = command_info.command
        cl = madfile.render(command, *conf_objects)

        xtra_info = {}
        xtra_info['cl'] = cl
        xtra_info['comm'] = command_info

        to_annotate = self.app.conf.plugin.x.annotate
        to_annotate.update(self.defaults.annotate)
        to_annotate.update(command_info.annotate)
        xtra_info['annotate'] = to_annotate

        conf_objects =  [xtra_info] + conf_objects

        script = madfile.render(self.conf.execscript,
                                *conf_objects)

        if '{{' in cl or '{%' in cl:
            lg.error("cannot render command line")
            lg.error(" cl: %s", cl)
            exit(-1)

        if '{{' in script or '{%' in script:
            lg.error("cannot render execution script")
            lg.error(" cl: %s", script)
            exit(-1)

        #print(self.defaults)
        lg.info("executing: {0}".format(cl))
        lg.debug("executing: {0}".format(script))

        return cl, script

    def execute(self, madfile, command):
        """
        Execute a single command line
        """
        def _applicator(cl, bg):
            if bg:
                sp.Popen(cl, shell=True)
            else:
                sp.call(cl, shell=True)

        cl, script = self.prepare_script(madfile, command)

        if self.args.dry:
            print(cl)
        else:
            rv = Yaco.Yaco()
            rv.cl = cl
            self.pool.apply_async(_applicator,
                                  (script, self.args.bg))

        return rv

    def finish(self):
        """
        Close & join the executor pool
        """
        self.pool.close()
        self.pool.join()
        lg.debug("finished executing")



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
        command_info = _get_command(app, madfile, command_name)
        if not command_info:
            print("Command {} does not exists for {}".format(
                command_name, madfile.basename))
            continue
        runinfo = executor.execute(madfile, command_info)

        t = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        h = madfile.mad.execute.history[t]
        h.update(runinfo)
        # process_type = execinfo.get('type', None)
        # if process_type == 'transform':
        #     handle_process_transform(madfile, execinfo)
        madfile.save()


    executor.finish()
