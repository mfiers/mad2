# -*- coding: utf-8 -*-
from __future__ import print_function,  unicode_literals
import logging
import os
import pkg_resources
import sys
import subprocess
import tempfile

import leip
import Yaco

import mad2.ui
from mad2.util import  get_mad_file, get_all_mad_files

from signal import signal, SIGPIPE, SIG_DFL

#Ignore SIG_PIPE and don't throw exceptions
#otherwise it crashes when you pipe into, for example, a head
#see http://newbebweb.blogspot.be/2012/02/python-head-ioerror-errno-32-broken.html
#see http://docs.python.org/library/signal.html
signal(SIGPIPE,SIG_DFL)
lg = logging.getLogger(__name__)

def dispatch():
    """
    Run the app - this is the actual entry point
    """
    app.run()

##
## define Mad commands
##

@leip.arg('file', nargs='?')
@leip.arg('key', help='key to set')
@leip.command
def edit(app, args):
    """
    Edit a key in a full screen editor
    """
    key = args.key
    editor = os.environ.get('EDITOR','vim')

    if args.file:
        madfile = get_mad_file(app, args.file)
        default = madfile.mad.get(key, "")
    else:
        #if no file is defined, use the configuration
        default = app.conf.get(key, "")

    #write default value to a temp file, and start the editor
    tmp_file = tempfile.NamedTemporaryFile('wb', delete=False)
    if default:
        tmp_file.write(default)
    tmp_file.close()
    subprocess.call([editor, tmp_file.name])

    #read tmp file
    with open(tmp_file.name, 'r') as F:
        #removing trailing space
        val = F.read().rstrip()

    if args.file:
        madfile.mad[key] = val
        madfile.save()
    else:
        app.conf[key] = val
        app.conf.save()

    os.unlink(tmp_file.name)


@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('file', nargs='*')
@leip.arg('value', help='value to set')
@leip.arg('key', help='key to set')
@leip.command
def set(app, args):

    key = args.key
    val = args.value

    madfiles = list(get_all_mad_files(app, args))

    if val == '-':
        if len(madfiles) == 1:
            data = madfiles[0].data(app.conf)
            default = madfiles[0].mad.get(key, "")
        else:
            data = app.conf.simple()

        val = mad2.ui.askUser(key, default, data)

    lg.debug("processing %d files" % len(madfiles))

    #if len(madfiles) == 0:
    #    #apply conf to the local user config if no madfiles are defined
    #    app.conf[key] = val
    #    app.conf.save()
    #    return

    for madfile in madfiles:
        list_mode = False
        if key[0] == '+':
            list_mode = True
            key = key[1:]

        keywords = app.conf.keywords
        if not args.force and not key in keywords:
            print("invalid key: {0} (use -f?)".format(key))
            sys.exit(-1)

        keyinfo = keywords[key]
        keytype = keyinfo.get('type', 'str')

        if list_mode and keyinfo.cardinality == '1':
            print("Cardinality == 1 - no lists!")
            sys.exit(-1)
        elif keyinfo.cardinality == '+':
            list_mode = True

        if keytype == 'restricted' and \
                not val in keyinfo.allowed:
            print("Value '{0}' not allowed".format(val))
            sys.exit(-1)

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
@leip.arg('-a', '--all', action='store_true')
@leip.arg('file', nargs='*')
@leip.command
def show(app, args):
    i = 0
    for madfile in get_all_mad_files(app, args):
        d = madfile.mad.copy()
        d.update(madfile.otf)
        if i > 0:
            print('---')
        print(d.pretty().strip())
        i += 1

##
## define show
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
        if args.key in madfile.mad:
            del(madfile.mad[args.key])
        madfile.save()


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

base_config = pkg_resources.resource_string('mad2', 'etc/mad.config')

#trail of config files???
config_files = [
    '/etc/mad.config',
    '~/.config/mad/mad.config']

path = os.getcwd()
config_no = 0
xtra_config = []
while path:
    config_no += 1
    xtra_config.append(os.path.join(path, '.mad'))
    path = path.rsplit(os.sep, 1)[0]

config_files.extend(list(reversed(xtra_config)))
app = leip.app(name='mad', set_name=None, base_config=base_config,
               config_files = config_files)

#discover hooks in this module!
app.discover(globals())
