# -*- coding: utf-8 -*-
from __future__ import print_function,  unicode_literals

from collections import Counter
import logging
import os
from signal import signal, SIGPIPE, SIG_DFL
import subprocess
import sys
import tempfile

from dateutil.parser import parse as dateparse

import leip
import Yaco
import mad2.ui

from mad2.util import  get_mad_file, get_all_mad_files


#Ignore SIG_PIPE and don't throw exceptions
#otherwise it crashes when you pipe into, for example, head
#see http://newbebweb.blogspot.be/2012/02/python-head-ioerror-errno-32-broken.html
#see http://docs.python.org/library/signal.html
signal(SIGPIPE, SIG_DFL)


lg = logging.getLogger(__name__)

def dispatch():
    """
    Run the app - this is the actual application entry point
    """
    app.run()


##
## define Mad commands
##

@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('-p', '--prompt', action='store_true', help='show a prompt')
@leip.arg('-e', '--editor', action='store_true', help='open an editor')
@leip.arg('file', nargs='*')
@leip.arg('value', help='value to set', nargs='?')
@leip.arg('key', help='key to set')
@leip.command
def set(app, args):
    """
    Set a key/value for one or more files.

    Use this command to set a key value pair for one or more files.

    This command can take the following forms::

        mad set project test genome.fasta
        ls *.fasta | mad set project test
        find . -size +10k | mad set project test


    """

    key = args.key
    val = args.value

    if args.prompt or args.editor:
        if not args.value is None:
            # when asking for a prompt - the value is assumed to be a file, and
            # needs to be pushed into args.file
            args.file = [args.value] + args.file

    madfiles = list(get_all_mad_files(app, args))

    if val is None and not (args.prompt or args.editor):
        args.prompt = True

    if args.prompt or args.editor:
        # get a value from the user

        default = ''
        #Show a prompt asking for a value
        if len(madfiles) == 1:
            data = madfiles[0].data(app.conf)
            default = madfiles[0].mad.get(key, "")
        else:
            data = app.conf.simple()

        if args.prompt:
            sys.stdin = open('/dev/tty')
            val = mad2.ui.askUser(key, default, data)
            sys.stdin = sys.__stdin__
        elif args.editor:
            editor = os.environ.get('EDITOR','vim')
            tmp_file = tempfile.NamedTemporaryFile('wb', delete=False)

            #write default value to the tmp file
            if default:
                tmp_file.write(default + "\n")
            else:
                tmp_file.write("\n")
            tmp_file.close()

            tty = open('/dev/tty')

            subprocess.call('{} {}'.format(editor, tmp_file.name),
                stdin=tty, shell=True)
            sys.stdin = sys.__stdin__


            #read value back in
            with open(tmp_file.name, 'r') as F:
                #removing trailing space
                val = F.read().rstrip()
            #remove tmp file
            os.unlink(tmp_file.name)


    lg.debug("processing %d files" % len(madfiles))

    list_mode = False
    if key[0] == '+':
        lg.info("treating {} as a list".format(key))
        list_mode = True
        key = key[1:]

    keywords = app.conf.keywords
    keyinfo = keywords[key]
    keytype = keyinfo.get('type', 'str')

    if list_mode and keyinfo.cardinality == '1':
        print("Cardinality == 1 - no lists!")
        sys.exit(-1)
    elif keyinfo.cardinality == '+':
        list_mode = True


    if not args.force and not key in keywords:
        print("invalid key: {0} (use -f?)".format(key))
        sys.exit(-1)


    if keytype == 'int':
        try:
            val = int(val)
        except ValueError:
            lg.error("Invalid integer: %s" % val)
            sys.exit(-1)
    elif keytype == 'float':
        try:
            val = float(val)
        except ValueError:
            lg.error("Invalid float: %s" % val)
            sys.exit(-1)
    elif keytype == 'boolean':
        if val.lower() in ['1', 'true', 't', 'yes', 'aye', 'y', 'yep']:
            val = True
        elif val.lower() in ['0', 'false', 'f', 'no', 'n', 'nope']:
            val = False
        else:
            lg.error("Invalid boolean: %s" % val)
            sys.exit(-1)
    elif keytype == 'date':
        try:
            val = dateparse(val)
        except ValueError:
            lg.error("Invalid date: %s" % val)
            sys.exit(-1)
        lg.warning("date interpreted as: %s" % val)

    if keytype == 'restricted' and \
            not val in keyinfo.allowed:
        print("Value '{0}' not allowed".format(val))
        sys.exit(-1)

    for madfile in madfiles:

        if list_mode:
            if not key in madfile.mad:
                oldval = []
            else:
                oldval = madfile.mad[key]
                if not isinstance(oldval, list):
                    oldval = [oldval]
            madfile.mad[key] = oldval + [val]
        else:
            #not listmode
            madfile.mad[key] = val

        madfile.save()

##
## define show
##
@leip.arg('-y', '--yaml', action='store_true')
@leip.arg('file', nargs='*')
@leip.command
def show(app, args):
    i = 0

    for madfile in get_all_mad_files(app, args):
        if i > 0:
            print('---')

        if args.yaml:
            print(madfile.all.pretty())
        else:
            for k in sorted(madfile.all.keys()):
                v = madfile.all[k]

                if isinstance(v, Yaco.Yaco):
                    continue
                print("{}\t{}".format(k,v))

        i += 1

##
## define unset
##
@leip.arg('file', nargs="+")
@leip.arg('key')
@leip.command
def unset(app, args):
    lg.debug("unsetting: %s".format(args.key))
    key = args.key
    keywords = app.conf.keywords
    keyinfo = keywords[key]
    if keyinfo.cardinality =='+':
        print("Not implemented - unsetting keys with cardinality > 1")
        sys.exit(-1)

    for madfile in get_all_mad_files(app, args):
        #print(madfile)
        #print(madfile.mad.pretty())
        if args.key in madfile.mad:
            del(madfile.mad[args.key])
        madfile.save()


@leip.commandName('key')
def madkeylist(app, args):
    kyw = sorted(app.conf.keywords)
    mxlen = str(max([len(x) for x in kyw])+1)
    for c in sorted(app.conf.keywords):
        print(("{:<" + mxlen + "} {}").format(c, app.conf.keywords[c].description))


@leip.commandName('category')
def madcatlist(app, args):
    kyw = sorted(app.conf.keywords.category.allowed)
    mxlen = str(max([len(x) for x in kyw])+1)
    for c in sorted(kyw):
        dsc = app.conf.keywords.category.allowed[c]
        print(("{:<" + mxlen + "} {}").format(c, dsc))
        tmpl = app.conf.template[c]
        if not tmpl: continue
        mxlen2 = str(max([len(x) for x in tmpl])+1)
        for k in sorted(tmpl):
            print(("  - {:<" + mxlen2 + "} {}").format(k, app.conf.template[c][k]))


@leip.arg('section', help='section to print', default='', nargs='?')
@leip.command
def sysconf(app, args):
    c = app.conf[args.section]
    print(c.pretty())

@leip.arg('comm', metavar='command', help='command to check')
@leip.command
def has_command(app, args):
    comm = args.comm
    lg.info("checking if we know command %s" % comm)
    if comm in app.leip_commands:
        sys.exit(0)
    else:
        sys.exit(-1)

##
## Instantiate the app and discover hooks & commands
##

#trail of config files???
config_files = [
    'pkg://mad2/etc/*.config',
    '/etc/mad/',
    '~/.config/mad/']

for c in config_files:
    lg.debug("using config file: {}".format(c))

# path = os.getcwd()
# config_no = 0
# xtra_config = []
# while path:
#     config_no += 1
#     xtra_config.append(os.path.join(path, '.mad'))
#     path = path.rsplit(os.sep, 1)[0]
# config_files.extend(list(reversed(xtra_config)))

app = leip.app(name='mad2', set_name=None,
               config_files = config_files)

#discover hooks in this module!
app.discover(globals())
