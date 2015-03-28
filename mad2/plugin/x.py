

import collections
import logging
from multiprocessing.dummy import Pool as ThreadPool
import os
import subprocess as sp
import uuid

import leip
import fantail

from mad2.util import get_all_mad_files
from mad2.plugin.template import render_numeric

lg = logging.getLogger(__name__)
# lg.setLevel(logging.DEBUG)


class Executor(object):

    def __init__(self, app, args):
        self.app = app
        self.args = args

        executor_type = args.executor

        self.executor_type = executor_type
        confName = executor_type.lower()

        self.conf = app.conf['x.executor.{}'.format(confName)]
        self.defaults = app.conf['x.executor.default']

        lg.debug("opening thread pool with %d threads",
                 args.threads)

        self.pool = ThreadPool(args.threads)

    def get_script_template(self, xtype):
        if xtype in self.conf:
            return self.conf[xtype]

        return self.defaults[xtype]

    def prepare_commandline(self, madfile, command_info):
        pass

    def prepare_script(self, madfileset, command_info):
        """
        prepare command line & full execution prepare_script

        """
        xtra_info = fantail.Fantail()
        xtra_info['input_file'] = madfileset[0]
        xtra_info['input_files'] = madfileset
        xtra_info['filename'] = madfileset[0]["filename"]
        xtra_info['comm'] = command_info

        lg.debug("filename: " + xtra_info['filename'])

        xtype = command_info.get('type', 'map')
        lg.debug("exec type %s", xtype)

        # first step - render the actual command
        x_conf = [xtra_info,
                  command_info['defaults'],
                  self.conf,
                  self.defaults,
                  self.app.conf['plugin.x.defaults'],
                  self.app.conf]

        # hack - should be possible from within jinja
        command = command_info['command']

        cl = madfileset[0].render(command, [madfileset[0]] + x_conf)

        xtra_info['cl'] = cl
        xtra_info['input_files'] = madfileset
        xtra_info['pwd'] = os.getcwd()
        xtra_info['comm'] = command_info
        xtra_info['uuid'] = str(uuid.uuid1())

        script = self.get_script_template(xtype)
        script = madfileset[0].render(script, [madfileset[0]] + x_conf)

        invoke_cmd = self.conf['invocator.execute_command']
        invoke_cmd = madfileset[0].render(invoke_cmd, [madfileset[0]] + x_conf)

        if ('{{' in cl) or ('{%' in cl):
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
        lg.debug(
            "script: {0}".format(script))
        lg.debug(
            "invocate: {0}".format(invoke_cmd))

        return cl, script, invoke_cmd

    def execute(self, madfileset, command):
        """
        Execute a single command line on a (set of) file(s)
        """

        def _applicator(invoke, script, bg):
            P = sp.Popen(invoke, shell=True, stdin=sp.PIPE)
            P.communicate(script)

        cl, script, invoke = \
            self.prepare_script(madfileset, command)

        if self.args.dry:
            print('#' * 80, 'invoke')
            print(invoke)
            print('#' * 80, 'execute')
            print(script)
            print('#' * 80)
        else:
            rv = fantail.Fantail()
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
        if command_name in app.conf['x.filetype'][filetype]:
            return app.conf['x.filetype'][filetype][command_name]

    #print(app.conf['x.filetype.default'].keys())
    return app.conf['x.filetype.default'][command_name]


@leip.arg('file', nargs='*')
@leip.command
def commands(app, args):
    """
    Commands available:
    """
    for madfile in get_all_mad_files(app, args):
        filetype = madfile.get('filetype', "")
        if filetype:
            print('{} ({})'.format(madfile['inputfile'], filetype))
        else:
            print('{} (unknown file format)'.format(madfile['inputfile']))

        for command_name in app.conf['x.filetype.default']:
            cinf = app.conf['x.filetype.default'][command_name]
            print(' g {0}: {1}'.format(command_name, cinf['description']))

        if not filetype:
            return

        for command_name in app.conf['x.filetype'][filetype]:
            cinf = app.conf['x.filetype'][filetype][command_name]
            print(' s {0}: {1}'.format(command_name,
                                       cinf.get('description', 'no description')))


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
        files = list(groups.values())
    else:
        files = [[x] for x in get_all_mad_files(app, args)]

    #print(list(f[0]['filename'] for f in files))

    for madfileset in files:

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



@leip.arg('-g', '--group', help='group on', default=1)
@leip.arg('file', nargs='*')
@leip.arg('comm', metavar='command', help='predefined command to execute')
@leip.command
def x2(app, args):
    lg.warning("x")

    groupby = args.group
    try:
        groupby = int(groupby)
        numeric_groups = True
    except:
        raise NotImplemented("Non numeric groups")

    mf_generator = get_all_mad_files(app, args)

    for res in render_numeric(app, mf_generator, args.comm, args.group):
        print(res)

# h()
# madfile.save()

#     executor.finish()
# h()
