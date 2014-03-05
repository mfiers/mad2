from __future__ import print_function

import collections
import datetime
import logging
from multiprocessing.dummy import Pool as ThreadPool
import os
import subprocess as sp
import sys
import uuid

import leip
import Yaco2
from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)
lg.setLevel(logging.DEBUG)

class Executor(object):

    def __init__(self, app, args):
        self.app = app
        self.args = args

        executor_type = args.executor

        self.executor_type = executor_type
        confName = executor_type.lower()

        self.conf = app.conf.get_branch('x.executor.{}'.format(confName))
        self.defaults = app.conf.get_branch('x.executor.default')

        lg.debug("opening thread pool with %d threads",
                 args.threads)

        self.pool = ThreadPool(args.threads)

    def prepare_script(self, madfileset, command_info):
        """
        prepare command line & full execution prepare_script

        """
        xtra_info = Yaco2.Yaco()
        xtra_info['input_file'] = madfileset[0]
        xtra_info['input_files'] = madfileset

        xtype = command_info.get('type', 'map')
        lg.debug("exec type %s", xtype)

        #first step - render the actual command
        x_conf = Yaco2.YacoStack(
            [ xtra_info,
              command_info.get_branch('defaults'),
              self.conf,
              self.defaults,
              self.app.conf.get_branch('plugin.x.defaults'),
              self.app.conf ])

        #hack - should be possible from within jinja
        x_conf['c'] = x_conf
        x_conf['comm'] = command_info
        command = command_info['command']

        cl = madfileset[0].render(command, x_conf)

        xtra_info['cl'] = cl
        xtra_info['input_files'] = madfileset
        xtra_info['pwd'] = os.getcwd()
        xtra_info['comm'] = command_info
        xtra_info['uuid'] = str(uuid.uuid1())

        # # to_annotate = self.app.conf.plugin.x.annotate
        # # to_annotate.update(self.defaults.annotate)
        # # to_annotate.update(command_info.annotate)

        # xtra_info['annotate'] = to_annotate

        script = madfileset[0].render(self.conf[xtype],
                                      x_conf)

        invoke_cmd = self.conf['invocator.execute_command']
        invoke_cmd = madfileset[0].render(invoke_cmd,
                                          x_conf)

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

    def execute(self, madfileset, command):
        """
        Execute a single command line on a (set of) file(s)
        """

        def _applicator(invoke, script, bg):
            P = sp.Popen(invoke, shell=True, stdin=sp.PIPE)
            P.communicate(script)

        cl, script, invoke = self.prepare_script(madfileset, command)

        if self.args.dry:
            print(invoke)
            print(script)
        else:
            rv = Yaco2.Yaco()
            rv['cl'] = cl
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

    filetype = madfile.get('filetype', "")
    if filetype:
        templates = Yaco2.YacoStack([
            app.conf.get_branch('x.filetype.{}'.format(filetype)),
            app.conf.get_branch('x.filetype.default')])
    else:
        templates = app.conf.get_branch('x.filetype.default')


    execinfo = templates.get_branch(command_name)
    return execinfo

@leip.arg('file', nargs='*')
@leip.command
def commands(app, args):
    """
    Commands available:
    """
    for madfile in get_all_mad_files(app, args):
        filetype = madfile.get('filetype', "")
        if filetype:
            print('{} ({})'.format(madfile['filename'], filetype))
            templates = Yaco2.YacoStack([
                app.conf.get_branch('x.filetype.{}'.format(filetype)),
                app.conf.get_branch('x.filetype.default')])
        else:
            print('{} (unknown filetype)'.format(madfile['filename']))
            templates = app.conf.get_branch('x.filetype.default')

        no_commands = 0
        for k in templates.keys(1):
            no_commands += 1
            command_info = templates.get_branch(k)

            print("- '{}': {}".format(k, command_info.get('description')))
        if no_commands == 0:
            print(" -- no commands available")


@leip.arg('file', nargs='*')
@leip.arg('comm', metavar='command', help='predefined command to execute')
@leip.arg('-j', '--threads', help='no of threads', type=int, default=1)
@leip.arg('-x', '--executor', help="executor to use", default='simple',
          choices=['simple', 'pbs'])
@leip.arg('-g', '--groupby',
          help='group on this field for combined execution')
@leip.flag('-d', '--dry', help='dry run')
@leip.flag('-b', '--bg', help='run in the background')
@leip.command
def x(app, args):
    """
    Execute a command
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

    for madfileset in files:

        for madfile in madfileset:
            command_info = _get_command(app, madfile, command_name)
            if not command_info:
                lg.error("Command {} does not exists for {}".format(
                         command_name, madfile.basename))
                exit(-1)

        runinfo = executor.execute(madfileset, command_info)

        #t = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        #h = madfile.mad.execute.history[t]
        # h.update(runinfo)
        # madfile.save()

    executor.finish()
