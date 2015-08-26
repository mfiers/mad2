

import collections
import functools

import pickle
import errno
import logging
import os
import re
import sys
import time

from termcolor import cprint
from pymongo import MongoClient

from mad2.exception import MadPermissionDenied, MadNotAFile
from mad2.madfile import MadFile, MadDummy

import fantail


lg = logging.getLogger(__name__)
#lg.setLevel(logging.DEBUG)

#
# Helper function - instantiate a madfile, and provide it with a
# method to run hooks
#

STORES = None
MONGODB = None
MONGO = None
MONGOCORE = None
MONGOTRANS = None


#
# Mongodb utils
#

def get_mongo_db(app):
    """
    Get the mongo database (no collection)
    """
    global MONGODB

    if MONGODB is not None:
        return MONGODB

    info = app.conf['store.mongo']
    host = info.get('host', 'localhost')
    port = info.get('port', 27017)
    dbname = info.get('db', 'mad2')

    lg.debug("connect mongodb %s:%s/%s", host, port, dbname)
    client = MongoClient(host, port)

    MONGODB = client[dbname]
    return MONGODB


def get_mongo_transact_db(app):
    """
    Get the core collection object
    """
    global MONGOTRANS

    if MONGOTRANS is not None:
        return MONGOTRANS

    db = get_mongo_db(app)

    info = app.conf['store.mongo']
    coll_t = info.get('transact_collection', 'transaction')
    coll_s2t = info.get('shasum2transact_collection', 'shasum2transaction')
    MONGOTRANS = (db[coll_t], db[coll_s2t])
    return MONGOTRANS


def get_mongo_core_db(app):
    """
    Get the core collection object
    """
    global MONGOCORE

    if MONGOCORE is not None:
        return MONGOCORE

    info = app.conf['store.mongo']
    host = info.get('host', 'localhost')
    port = info.get('port', 27017)
    dbname = info.get('db', 'mad2')
    coll = info.get('collection', 'core')
    lg.debug("connect mongodb %s:%s/%s/%s", host, port, dbname, coll)
    client = MongoClient(host, port)

    MONGOCORE = client[dbname][coll]

    return MONGOCORE


def get_mongo_transient_db(app):
    """
    Get the collection object
    """
    global MONGO

    if MONGO is not None:
        return MONGO

    mongo_info = app.conf['store.mongo']
    host = mongo_info.get('host', 'localhost')
    port = mongo_info.get('port', 27017)
    dbname = mongo_info.get('db', 'mad2')
    coll = mongo_info.get('transient_collection', 'transient')

    lg.debug("connect mongodb {}:{}".format(host, port))
    client = MongoClient(host, port)

    MONGO = client[dbname][coll]

    return MONGO


def persistent_cache(path, cache_on, duration):
    """
    Disk persistent cache that reruns a function once every
    'duration' no of seconds
    """
    def decorator(original_func):

        def new_func(*args, **kwargs):

            if isinstance(cache_on, str):
                cache_name = kwargs[cache_on]
            elif isinstance(cache_on, int):
                cache_name = args[cache_on]

            full_cache_name = os.path.join(path, cache_name)
            lg.debug("cache file: %s", full_cache_name)
            run = False

            if kwargs.get('force'):
                run = True

            if not os.path.exists(full_cache_name):
                #file does not exist. Run!
                run = True
            else:
                #file exists - but is it more recent than
                #duration (in seconds)
                mtime = os.path.getmtime(full_cache_name)
                age = time.time() - mtime
                if age > duration:
                    lg.debug("Cache file is too recent")
                    lg.debug("age: %d", age)
                    lg.debug("cache refresh: %d", duration)
                    run = True

            if not run:
                #load from cache
                lg.debug("loading from cache: %s", full_cache_name)
                with open(full_cache_name, 'rb') as F:
                    try:
                        res = pickle.load(F)
                        return res
                    except EOFError:
                        lg.warning("problem loading cached object")
                        os.unlink(full_cache_name)


            #no cache - create
            lg.debug("no cache - running function %s", original_func)
            rv = original_func(*args, **kwargs)
            lg.debug('write to cache: %s', full_cache_name)

            if not os.path.exists(path):
                os.makedirs(path)
            try:
                with open(full_cache_name, 'wb', pickle.HIGHEST_PROTOCOL) as F:
                    pickle.dump(rv, F)
            except:
                print(rv)
                raise

            return rv

        return new_func

    return decorator


def initialize_stores(app):
    # prevent circular import
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


def get_mad_file(app, filename, sha1sum=None):
    """
    Instantiate a mad file & add hooks
    """
    global STORES
    if STORES is None:
        initialize_stores(app)

    lg.debug("instantiating madfile for {0}".format(filename))
    return MadFile(filename,
                   stores=STORES,
                   sha1sum=sha1sum,
                   base=app.conf['madfile'],
                   hook_method=app.run_hook)


def get_mad_dummy(app, data):
    """
    instantiate a dummy - only used to save.

    """

    global STORES
    if STORES is None:
        initialize_stores(app)

    if 'madname' in data:
        del data['madname']
    if 'fullmadpath' in data:
        del data['fullmadpath']

    data_all = fantail.Fantail(data)
    data_core = fantail.Fantail()

    for kw in list(data_all.keys()):
        tra = app.conf['keywords'][kw].get('transient', False)
        if not tra:
            data_core[kw] = data_all[kw]

    lg.debug("instantiating dummy madfile")
    return MadDummy(data_all=data_all, data_core=data_core, stores=STORES,
                    hook_method=app.run_hook )

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
            if len(f) == 0:
                continue
            if '.mad/' in f:
                continue
            if 'SHA1SUMS' in f:
                continue
            if 'SHA1SUMS.META' in f:
                continue
            if 'QDSUMS' in f:
                continue


            if not os.access(f, os.R_OK):
                # no read access - ignore this file
                continue


            try:
                os.stat(f)
            except OSError as e:
                lg.warning("Problem getting stats from %s", f)
                if e.errno == errno.ENOENT:
                    # path does not exists - or is a broken symlink
                    continue
                else:
                    raise

            lg.debug("processing %s", f)

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
            if os.path.isdir(rv):
                continue
            yield rv


def get_all_mad_files(app, args, use_stdin=True, warn_on_errors=True):
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
        except Exception as e:
            if warn_on_errors:
                lg.warning("Error instantiating %s", filename)
                lg.warning("Error: %s", str(e))
                raise
            else:
                raise

# Thanks: http://tinyurl.com/kq5hxtr

def interpret_humansize(s):
    s = s.strip().lower()
    if s.endswith('mb'):
        return int(s[:-2]) * ( 1024 ** 2)
    elif s.endswith('gb'):
        return int(s[:-2]) * ( 1024 ** 3)
    elif s.endswith('kb'):
        return int(s[:-2]) * ( 1024)
    elif s.endswith('tb'):
        return int(s[:-2]) * ( 1024 ** 4)
    else:
        return int(s)


def humansize(nbytes):
    import numpy as np
    if np.isnan(nbytes):
        return float("nan")
    suffixes = [' b', 'kb', 'Mb', 'Gb', 'Tt', 'Pb']
    if nbytes == 0:
        return '0 b'
    i = 0
    while nbytes >= 1024 and i < len(suffixes) - 1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('.')
    return '%s %s' % (f, suffixes[i])


def boolify(v):
    """
    return a boolean from a string
    yes, y, true, True, t, 1 -> True
    otherwise -> False
    """
    return v.lower() in ['yes', 'y', 'true', 't', '1']


def message(cat, message, *args):
    if len(args) > 0:
        message = message.format(*args)

    message = " ".join(message.split())
    color = {'er': 'red',
             'wa': 'yellow',
             'in': 'green',
             }.get(cat.lower()[:2], 'blue')

    cprint('Kea', 'cyan', end="/")
    cprint(cat, color)
    for line in textwrap.wrap(message):
        print("  " + line)


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
