
from __future__ import print_function,  unicode_literals
import sys
import leip

from mad2.madfile import MadFile
import logging

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
# @leip.hook("madfile_save")
# def check_shasum(app, madfile):
#     lg.debug("check shasum for %s" % madfile)
#     if not 'checksum' in madfile.mad:
#         madfile.checksum()


## 
## Helper function - instantiate a madfile, and provide it with a
## method to run hooks
##
def get_mad_file(app, filename):
    """
    Instantiate a mad file & add hooks
    """
    madfile = MadFile(filename)

    def run_hook(hook_name):
        app.run_hook(hook_name, madfile)
        
    madfile.hook_method = run_hook
    return madfile

##
## define Mad commands
##
@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('file', nargs='+')
@leip.arg('value', help='value to set')
@leip.arg('key', help='key to set')
@leip.command
def set(app, args):
    for filename in args.file:
        lg.debug("processing file: %s" % filename)
        madfile = get_mad_file(app, filename)
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
    for filename in args.file:
        madfile = get_mad_file(app, filename)
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
