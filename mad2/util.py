
import logging
import select
import sys
import re

from mad2.madfile import MadFile

lg = logging.getLogger(__name__)

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


def get_filenames(args):
    """
    Get all incoming filenames
    """
    filenames = []
    demad = re.compile(r'\.mad$')
    if 'file'in args and len(args.file) > 0:
        filenames.extend([demad.sub('', x)
                         for x in args.file])
    else:
        #nothing in args - wait for stin
        filenames.extend([demad.sub('', x)
                          for x in sys.stdin.read().split()])

    filenames = sorted(list(set(filenames)))
    return filenames

def get_all_mad_files(app, args):
    """
    get input files from sys.stdin and args.file
    """
    for filename in get_filenames(args):
        yield get_mad_file(app, filename)
