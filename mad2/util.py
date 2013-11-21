import logging
import re
import select
import sys

from mad2.exception import MadPermissionDenied, MadNotAFile
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
    lg.debug("instantiating madfile for {0}".format(filename))
    return MadFile(filename, base=app.conf.madfile, hook_method = app.run_hook)

def get_filenames(args):
    """
    Get all incoming filenames
    """
    filenames = []

    demad = re.compile(r'^(?P<path>.*?/)?\.(?P<fn>[^/].+)\.mad$')
    def demadder(m):
        if not m.group('path') is None:
            return '{}{}'.format(m.group('path'), m.group('fn'))
        else:
            return m.group('fn')

    if 'file'in args and len(args.file) > 0:
        filenames.extend([demad.sub(demadder, x)
                         for x in args.file
                         if (len(x) > 0 and not '.mad/' in x)])
    else:
        #nothing in args - see if there is something on stdin
        filenames.extend(
                [demad.sub(demadder, x)
                 for x in sys.stdin.read().split("\n")
                 if (len(x) > 0 and not '.mad/' in x)])

    filenames = sorted(list(set(filenames)))

    #remove directories as well
    filenames = [x for x in filenames if not '.mad' in x]

    return filenames

def get_all_mad_files(app, args):
    """
    get input files from sys.stdin and args.file
    """
    for filename in get_filenames(args):
        try:
            yield get_mad_file(app, filename)
        except MadNotAFile:
            pass
        except MadPermissionDenied:
            lg.warning("Permission denied: {}".format(
                filename))

def boolify(v):
    """
    return a boolean from a string
    yes, y, true, True, t, 1 -> True
    otherwise -> False
    """
    return v.lower() in ['yes', 'y', 'true', 't', '1']

def render(txt, data):

    env = moa.moajinja.getStrictEnv()
    renconf = self.render()
    templ = env.from_string(value)
    try:
        rv = templ.render(renconf)
        return rv
    except jinja2.exceptions.UndefinedError:
        return value
    except jinja2.exceptions.TemplateSyntaxError:
        return value
