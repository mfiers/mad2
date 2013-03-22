
from __future__ import print_function,  unicode_literals
import logging
import sys
import select

from mad2.madfile import MadFile
import leip


lg = logging.getLogger(__name__)

def dispatch():
    """
    Run the app - this is the actual entry point
    """
    app.run()

##
## First, define hooks and make them discoverable using the @leip.hook
## decorator. 
##
@leip.hook("madfile_save")
def check_shasum(app, madfile):
    lg.debug("check shasum for %s" % madfile)
    command = app.trans.args.command
    if command in ['catchup', 'defer']:
        #do not do this when cathcing up or deferring
        return
    if not 'checksum' in madfile.mad:
        madfile.defer('mad checksum {{filename}}')

## 
## Helper function - instantiate a madfile, and provide it with a
## method to run hooks
##
def get_mad_file(app, filename):
    """
    Instantiate a mad file & add hooks
    """
    lg.debug("instantiating madfile for {}".format(filename))
    madfile = MadFile(filename)

    def run_hook(hook_name):
        app.run_hook(hook_name, madfile)
        
    madfile.hook_method = run_hook
    return madfile

def get_all_mad_files(app, args):
    """
    get input files from sys.stdin and args.file
    """
    if select.select([sys.stdin,],[],[],0.0)[0]:
        for filename in sys.stdin.read().split():
            yield get_mad_file(app, filename)
    if 'file'in args and len(args.file) > 0:
        for filename in args.file:
            yield get_mad_file(app, filename)

##
## define Mad commands
##

@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('file', nargs='*')
@leip.command
def checksum(app, args):
    """
    Calculate a checksum
    """
    for madfile in get_all_mad_files(app, args):
        if not args.force and 'checksum' in madfile.mad:
            #exists - and not forcing
            lg.warning("Skipping checksum - exists")
            continue
        madfile.checksum()
        madfile.save()

@leip.arg('file', nargs='*')
@leip.command
def catchup(app, args):
    """
    execute all deferred commands
    """
    for madfile in get_all_mad_files(app, args):
        madfile.catchup()


@leip.arg('file', nargs='*')
@leip.arg('command', help='command to execute')
@leip.command
def defer(app, args):
    """
    defer a command for later execution (using mad catchup)

    An example command would be:
 
    """
    for madfile in get_all_mad_files(app, args):
        madfile.defer(command)
        madfile.save()


@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('file', nargs='*')
@leip.arg('value', help='value to set')
@leip.arg('key', help='key to set')
@leip.command
def set(app, args):
    for madfile in get_all_mad_files(app, args):

        key = args.key
        val = args.value

        list_mode = False
        if key[0] == '+':
            list_mode = True
            key = key[1:]
            
        keywords = app.conf.keywords
        if not args.force and not key in keywords:
            print("invalid key: {}".format(key))

        keyinfo = keywords[key]
        keytype = keyinfo.get('type', 'str')
        
        if list_mode and keyinfo.cardinality == '1':
            print("Cardinality == 1 - no lists!")
            sys.exit(-1)

        if keytype == 'restricted' and \
                not val in keyinfo.allowed:
            print("Value '{}' not allowed".format(val))
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
@leip.arg('file')
@leip.command
def show(app, args):
    lg.debug("processing file: %s" % args.file)
    madfile = get_mad_file(app, args.file)
    print(madfile.pretty().decode())
        
##
## define show
##
@leip.arg('file', nargs="+")
@leip.arg('key')
@leip.command
def unset(app, args):
    lg.debug("unsetting: %s".format(args.key))
    for madfile in get_all_mad_files(app, args):
        if args.key in madfile.mad:
            del(madfile.mad[args.key])
        madfile.save()

##        
## define find
##
@leip.arg('dir', default='.', help='directory to search from')
@leip.command
def find(app, args):
    lg.info("searching dir %s" % args.dir)

## 
## Instantiate the app and discover hooks & commands
##

import pkg_resources
base_config = pkg_resources.resource_string('mad2', 'etc/mad2.config')

app = leip.app(name='mad2', set_name='config', base_config = base_config)
#discover hooks in this module!
app.discover(globals())
