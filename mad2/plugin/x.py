from __future__ import print_function

import datetime
import logging
from multiprocessing.dummy import Pool as ThreadPool
import os
import subprocess as sp
import uuid

import leip
import Yaco
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)


class Executor(object):

    def __init__(self, app, args):
        self.app = app
        self.args = args

        executor_type=args.executor

        self.executor_type = executor_type
        confName = executor_type.capitalize() + "Executor"
        self.conf = app.conf.plugin.x[confName]
        self.defaults = app.conf.plugin.x[confName].defaults
        self.xdefaults = app.conf.plugin.x.defaults

        lg.debug("opening thread pool with %d threads",
                args.threads)
        self.pool = ThreadPool(args.threads)


    def prepare_script(self, madfile, command_info):
        """
        prepare command line & full execution prepare_script

        """

        conf_objects = [self.app.conf,
                        command_info.defaults,
                        self.defaults,
                        self.xdefaults,
                        self.conf  ]

        xtype = command_info.get('type', 'map')

        command = command_info.command

        cl = madfile.render(command, *conf_objects)

        xtra_info = {}
        xtra_info['cl'] = cl
        xtra_info['pwd'] = os.getcwd()
        xtra_info['comm'] = command_info
        xtra_info['uuid'] = str(uuid.uuid1())
        to_annotate = self.app.conf.plugin.x.annotate
        to_annotate.update(self.defaults.annotate)
        to_annotate.update(command_info.annotate)

        xtra_info['annotate'] = to_annotate

        conf_objects =  [xtra_info] + conf_objects

        script = madfile.render(self.conf[xtype],
                                *conf_objects)

        invoke_cmd = self.conf.invocator.execute_command
        invoke_cmd = madfile.render(invoke_cmd,
                                    *conf_objects)

        if '{{' in cl or '{%' in cl:
            lg.error("cannot render command line")
            lg.error(" cl: %s", cl)
            exit(-1)

        if '{{' in script or '{%' in script:
            lg.error("cannot render execution script")
            lg.error(" cl: %s", script)
            exit(-1)

        if '{{' in invoke_cmd or '{%' in invoke_cmd:
            lg.error("cannot render invocation script")
            lg.error(" cl: %s", invoke_cmd)
            exit(-1)

        lg.debug("cl: {0}".format(cl))
        lg.debug("script: {0}".format(script))
        lg.debug("invocate: {0}".format(invoke_cmd))

        return cl, script, invoke_cmd

    def execute(self, madfile, command):
        """
        Execute a single command line
        """
        def _applicator(invoke, script, bg):
            P = sp.Popen(invoke, shell=True, stdin=sp.PIPE)
            P.communicate(script)

        cl, script, invoke = self.prepare_script(madfile, command)

        if self.args.dry:
            print(invoke)
            print(script)
        else:
            rv = Yaco.Yaco()
            rv.cl = cl
            self.pool.apply_async(_applicator,
                                  (invoke, script, self.args.bg))

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
@leip.arg('-j', '--threads', help='no of threads', type=int, default=1)
@leip.arg('-x', '--executor', help="executor to use", default='simple',
          choices=['simple', 'pbs'])
@leip.flag('-d', '--dry', help='dry run')
@leip.flag('-b', '--bg', help='run in the background')
@leip.command
def x(app, args):
    """
    Execute a command
    """
    command_name = args.comm

    executor = Executor(app, args)

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
