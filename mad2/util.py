

import collections
import functools

import errno
import logging
import os
import re
import select
import sys

from mad2.exception import MadPermissionDenied, MadNotAFile
from mad2.madfile import MadFile

import fantail


lg = logging.getLogger(__name__)


# lg.setLevel(logging.DEBUG)

#
# Helper function - instantiate a madfile, and provide it with a
# method to run hooks
#

STORES = None

def initialize_stores(app):
    #prevent circular import
    import mad2.store

    global STORES
    STORES = {}
    for store in app.conf['store']:
        store_conf = app.conf['store'][store]
        if not isinstance(store_conf, dict):
            continue
        if not store_conf.get('enabled', False):
            continue
        STORES[store] = mad2.store.all_stores[store](store_conf)

def cleanup_stores(app):
    if STORES is None:
        return

    for store_name in STORES:
        store = STORES[store_name]
        store.finish()

def get_mad_file(app, filename):
    """
    Instantiate a mad file & add hooks
    """
    if STORES == None:
        initialize_stores(app)

    lg.debug("instantiating madfile for {0}".format(filename))
    return MadFile(filename,
                   stores = STORES,
                   base=app.conf['madfile'],
                   hook_method=app.run_hook)


def to_mad(fn):
    if '/' in fn:
        a, b = fn.rsplit('/')
        return os.path.join(a, '.{}.mad'.format(b))
    else:
        return '.{}.mad'.format(fn)


def get_filenames(args, use_stdin=True, allow_dirs=False):
    """
    Get all incoming filenames
    """
    filenames = []
    demad = re.compile(r'^(?P<path>.*/)?\.(?P<fn>[^/].+)\.mad$')

    def demadder(m):
        if not m.group('path') is None:
            return '{}{}'.format(m.group('path'), m.group('fn'))
        else:
            return m.group('fn')

    if 'file' in args and len(args.file) > 0:
        for f in args.file:
            if len(f) == 0: continue
            if '.mad/' in f: continue
            if 'SHA1SUMS' in f: continue
            if 'SHA1SUMS.META' in f: continue
            if 'QDSUMS' in f: continue

            if not os.access(f, os.R_OK):
                #no read access - ignore this file
                continue

            try:
                os.stat(f)
            except OSError, e:
                lg.warning("Problem getting stats from %s", f)
                if e.errno == errno.ENOENT:
                    #path does not exists - or is a broken symlink
                    continue
                else:
                    raise

            rv = demad.sub(demadder, f)

            if not allow_dirs and os.path.isdir(rv):
                continue

            yield rv

    elif use_stdin:

        # nothing in args - see if there is something on stdin
        for line in sys.stdin:
            line = line.strip()
            if '.mad/' in line:
                continue
            rv = demad.sub(demadder, line)
            if os.path.isdir(rv): continue
            yield rv


def get_all_mad_files(app, args, use_stdin=True):
    """
    get input files from sys.stdin and args.file
    """

    for filename in get_filenames(args, use_stdin):
        try:
            maf = get_mad_file(app, filename)
            yield maf
        except MadNotAFile:
            pass
        except MadPermissionDenied:
            lg.warning("Permission denied: {}".format(
                filename))

#Thanks: http://tinyurl.com/kq5hxtr
def humansize(nbytes):
    suffixes = [' b', 'kb', 'Mb', 'Gb', 'Tt', 'Pb']
    if nbytes == 0: return '0  b'
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])


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


# Borrowed from: http://tinyurl.com/majcr53
class memoized(object):
   '''Decorator. Caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned
   (not reevaluated).
   '''
   def __init__(self, func):
      self.func = func
      self.cache = {}
   def __call__(self, *args):
      if not isinstance(args, collections.Hashable):
         # uncacheable. a list, for instance.
         # better to not cache than blow up.
         return self.func(*args)
      if args in self.cache:
         return self.cache[args]
      else:
         value = self.func(*args)
         self.cache[args] = value
         return value
   def __repr__(self):
      '''Return the function's docstring.'''
      return self.func.__doc__
   def __get__(self, obj, objtype):
      '''Support instance methods.'''
      return functools.partial(self.__call__, obj)

# @memoized
# def fibonacci(n):
#    "Return the nth fibonacci number."
#    if n in (0, 1):
#       return n
#    return fibonacci(n-1) + fibonacci(n-2)

# print fibonacci(12)
