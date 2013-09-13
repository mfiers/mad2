import logging
import re
import select
import sys

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
    madfile = MadFile(filename,
                hook_method = app.run_hook)
    return madfile


def get_filenames(args):
    """
    Get all incoming filenames
    """
    filenames = []
    demad = re.compile(r'^\.(.*)\.mad$')

    if 'file'in args and len(args.file) > 0:
        filenames.extend([demad.sub(r'\1', x)
                         for x in args.file])
    else:
        #nothing in args - see if there is something on stdin
        filenames.extend(
                [demad.sub(r'\1', x)
                 for x in sys.stdin.read().split()])

    filenames = sorted(list(set(filenames)))
    return filenames

def get_all_mad_files(app, args):
    """
    get input files from sys.stdin and args.file
    """
    for filename in get_filenames(args):
        yield get_mad_file(app, filename)


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
