from __future__ import print_function

import collections
import copy

from fnmatch import fnmatch
import glob
import logging
import os
import re
import shutil
import sys
import time

import socket

import hashlib

import arrow
import datetime
import pytimeparse
import pymongo
import pandas as pd
from termcolor import cprint
import yaml

import leip

import mad2.hash
from mad2.util import get_all_mad_files, humansize, persistent_cache
import mad2.util
from mad2.util import get_mongo_transient_db, get_mongo_core_db
from mad2.util import get_mongo_transact_db, get_mongo_db
from mad2.ui import message


lg = logging.getLogger(__name__)
#lg.setLevel(logging.DEBUG)
COUNTER = collections.defaultdict(lambda: 0)
MONGO_SAVE_CACHE = []
MONGO_SAVE_COUNT = 0
MONGO_REMOVE_COUNT = 0

def get_mongo_transient_id(mf):
    hsh = hashlib.sha1()
    hsh.update(mf['host'].encode('UTF-8'))
    hsh.update(mf['fullpath'].encode('UTF-8'))
    return hsh.hexdigest()[:24]


def mongo_prep_mad(mf):

    mongo_id = get_mongo_transient_id(mf)

    d = dict(mf)
    d['_id'] = mongo_id
    if 'uuid' in d:
        del d['uuid']
    if 'hash' in d:
        del d['hash']
    d['save_time'] = datetime.datetime.utcnow()

    return mongo_id, d


MONGO_SAVE_CACHE = []
MONGO_REMOVE_CACHE = []


@leip.hook("flush")  # one can call this function as a hook!
def mongo_flush(app):

    global MONGO_REMOVE_CACHE
    global MONGO_SAVE_CACHE
    global COUNTER

    lg.debug("flush")
    if (len(MONGO_SAVE_CACHE) + len(MONGO_REMOVE_CACHE)) == 0:
        lg.debug("nothing to flush")
        return

    collection = get_mongo_transient_db(app)

    if len(MONGO_SAVE_CACHE) > 0:
        bulk = collection.initialize_unordered_bulk_op()
        for i, r in MONGO_SAVE_CACHE:
            COUNTER['saved'] += 1
            bulk.find({'_id': i}).upsert().replace_one(r)

        res = bulk.execute()
        lg.debug("Saved %d records", res['nModified'])

    for i, r in enumerate(MONGO_REMOVE_CACHE):
        # should try to do this in bulk, but uncertain how...
        COUNTER['removed'] += 1
        lg.info("removing id: %s", r)
        collection.remove({'_id': r})

    MONGO_SAVE_CACHE = []
    MONGO_REMOVE_CACHE = []


def save_to_mongo(app, madfile):
    global MONGO_SAVE_COUNT
    global MONGO_SAVE_CACHE
    global MONGO_REMOVE_CACHE
    global MONGO_REMOVE_COUNT

    MONGO_SAVE_COUNT += 1

    mongo_id, newrec = mongo_prep_mad(madfile)

    if madfile.get('orphan'):
        MONGO_REMOVE_CACHE.append(mongo_id)
        lg.info("removing %s from transient db", madfile['inputfile'])
    else:
        # rint(newrec['host'])
        lg.debug("saving to mongodb with id %s", mongo_id)
        MONGO_SAVE_CACHE.append((mongo_id, newrec))

    lg.debug("prep for save: %s", madfile['inputfile'])
    if len(MONGO_SAVE_CACHE) + len(MONGO_REMOVE_CACHE) > 20:
        mongo_flush(app)


@leip.hook("madfile_init", 200)
def madfile_init(app, madfile):
    """
    Initialize this madfile - mainly - check if the mongo transient database
    knows about this file, and has the SHA1SUM. The SHA1SUM is then used to get
    the data from the core database
    """
    global COUNTER
    COUNTER['init'] += 1

    trans_db = get_mongo_transient_db(app)
    core_db = get_mongo_core_db(app)

    trans_id = get_mongo_transient_id(madfile)
    rec = trans_db.find_one({'_id': trans_id})
    nowtime = datetime.datetime.utcnow()
    mtime = madfile.get('mtime')
    sha1sum = None
    sha1sum_time = None

    #lg.setLevel(logging.DEBUG)

    if isinstance(rec, dict):
        sha1sum = rec.get('sha1sum')
        sha1sum_time = rec.get('sha1sum_time')

    def _prep_madfile(_madfile, sha1, sha1_time):

        _madfile.all['_id_core'] = sha1[:24]
        _madfile.all['sha1sum'] = sha1
        _madfile.mad['sha1sum'] = sha1
        _madfile.all['sha1sum_time'] = sha1_time

    def _create_new_sha1(_madfile):

        # TODO: temporary hack - see if we can get the data from the
        # SHA1SUM files.

        sha1, sha1_time = mad2.hash.check_sha1sum_file(_madfile['fullpath'])

        if sha1 is not None and arrow.get(mtime).to('local') <= sha1_time:
            COUNTER['shafile'] += 1
            lg.debug("recoved sha1 from the SHA1SUM file")
        else:
            #also not in the sha1sum file - recalculate
            lg.debug("recreate shasum for %s", _madfile['inputfile'])
            COUNTER['calc'] += 1
            sha1 = mad2.hash.get_sha1(_madfile['fullpath'])
            sha1_time = datetime.datetime.utcnow()

        if sha1 is None:
            #still not?? maybe the file does not exist? Link is broken?? Will not save this
            return False

        lg.debug("shasum for %s (%s) is %s", _madfile['inputfile'], trans_id, sha1)

        trans_db.update({'_id': trans_id},
                        {"$set": {'sha1sum': sha1,
                                  'sha1sum_time': nowtime}},
                        upsert=True)
        _prep_madfile(madfile, sha1, sha1_time)
        return sha1


    if sha1sum is None or not(isinstance(sha1sum_time, datetime.datetime)):
        # no shasum - recreate
        _create_new_sha1(madfile)
    elif sha1sum_time is None or mtime is None or  mtime > sha1sum_time:
        # changed sha1sum?
        old_sha1sum = sha1sum
        new_sha1sum = _create_new_sha1(madfile)

        if old_sha1sum == new_sha1sum:
            COUNTER['unchanged'] += 1
        else:
            #record has changed - copy the core data from the old to the
            #new record.

            old_core_id = old_sha1sum[:24]
            new_core_id = new_sha1sum[:24]

            lg.info("file changed: %s", madfile['inputfile'][-30:])
            lg.debug("coreid %s -> %s", old_core_id, new_core_id)

            #prepare record
            old_core_record = core_db.find_one({'_id': old_core_id})
            if not old_core_record:
                old_core_record = {}
            if not 'old_sha1sums' in old_core_record:
                old_core_record['old_sha1sums'] = []
            old_core_record['old_sha1sums'].append(old_sha1sum)
            old_core_record['sha1sum'] = new_sha1sum
            if '_id' in old_core_record:
                del old_core_record['_id']

            #store in core database
            core_db.update({'_id': new_sha1sum[:24]},
                           {"$set": old_core_record},
                           upsert=True)
            madfile.mad.update(old_core_record)

            save_to_mongo(app, madfile)
            COUNTER['changed'] += 1
    else:
        _prep_madfile(madfile, sha1sum, sha1sum_time)


@leip.arg('-w', '--watch', action='store_true')
@leip.arg('-s', '--min_file_size', type=int, default=100,
          help='minimal file size to consider')
@leip.command
def update(app, args):
    """
    update the transient db in this directory and below
    """

    global MONGO_REMOVE_CACHE
    global MONGO_SAVE_CACHE
    global COUNTER

    modfiles = collections.deque([], 5)
    newfiles = collections.deque([], 5)

    transient_db = get_mongo_transient_db(app)
    ignore_dirs = ['.*', '.git', 'tmp']
    ignore_files = ['.*', '*.log', '*~', '*#', 'SHA1SUMS*', 'mad.config']
    basedir = os.getcwd()

    find_dir_regex = '^{}'.format(basedir)
    lg.debug("searching for dirs with regex: %s", find_dir_regex)
    tradirs = []
    query = {'host': socket.gethostname(),
             'dirname': { "$regex": find_dir_regex }}

    trans_dirs = list(transient_db.find(query).distinct('dirname'))
    lg.warning("found %d files below this directory in transient db", len(trans_dirs))

    #to be safe - strip trailing slashes
    trans_dirs = [x.rstrip('/') for x in trans_dirs]
    dirs_to_delete = copy.copy(trans_dirs)
    lg.info("%d dirs with data in the transient db", len(trans_dirs))


    def screen_update(cnt, lud = 0, msg=""):
        ts = shutil.get_terminal_size().columns - 1

        if (time.time() > lud) < 1:
            return lud

        def _add_sep(b):
            return re.sub(r'([0-9][0-9][0-9])', r'\1,', str(b)[::-1])[::-1].strip(',')

        if len(cnt) == 0:
            return time.time()

        out = " ".join(['{}:{}'.format(a, _add_sep(b))
                        for a, b in cnt.items()])

        rest = ts - (len(out) + 1)

        if rest > 5 and isinstance(msg, str):
            out += ':' + msg[:int(rest)-1]

        rest = ts - (len(out) + 1)
        out += ' ' * rest

        print(out, end='\r')

        return time.time()

    start = time.time()

    last_screen_update = screen_update(COUNTER, msg='init')


    def _name_match(fn, ignore_list):
        for i in ignore_list:
            if fnmatch(fn, i):
                return True
        return False

    for root, dirs, files in os.walk(basedir):
        last_screen_update = screen_update(COUNTER, lud=last_screen_update, msg=root)

        root = root.rstrip('/')
        COUNTER['dir'] += 1

        if os.path.exists(os.path.join(basedir, 'mad.ignore')):
            dirs[:] = []
            must_save_files = []
        else:
            dirs[:] = [x for x in dirs if not _name_match(x, ignore_dirs)]
            dirs[:] = [x for x in dirs if not os.path.exists(os.path.join(x, 'mad.ignore'))]

            must_save_files = [x for x in files if not _name_match(x, ignore_files)]


        def check_access(_root, _fn):
            _path = os.path.join(_root, _fn)
            acc = os.access(_path, os.R_OK)
            if not acc:
                COUNTER['no_access'] += 1
                if COUNTER['no_access'] < 10:
                    lg.info("no access to: %s", _path)
            return acc

        must_save_files = [x for x in must_save_files if check_access(root, x)]

        remove_dir = True

        if len(must_save_files) > 0:
            lg.debug('files to be saved in %s', root)
            remove_dir = False

        if args.watch:
            sys.stdout.write(chr(27) + "[2J" + chr(27) + "[1;1f")
            print(datetime.datetime.now())
            print()
            print("dir: {}".format(root))
            for k in sorted(COUNTER.keys()):
                print("  {:<20}:{:<10d}".format(k, COUNTER[k]))

        else:
            lg.info('%s: %s', root[-40:], str(dict(COUNTER)))

        trans_records = transient_db.find(
            { "dirname": root,
              "host": socket.gethostname(), },
            { "_id_transient": 1,
              "filename": 1,
              "sha1sum": 1,
              "sha1sum_time": 1, })

        trec_files = []

        for trec in trans_records:

            last_screen_update = screen_update(COUNTER, lud=last_screen_update, msg=root)

            if not trec['filename'] in files:
                COUNTER['rm'] += 1
                lg.debug('deleted: %s', trec['filename'])
                MONGO_REMOVE_CACHE.append(trec['_id_transient'])
                continue

            # this file is in both the db & on disk - check mtime
            remove_dir = False  # stuff in this folder - do not delete!
            fullpath = os.path.abspath(os.path.realpath(os.path.join(root, trec['filename'])))
            fstat = os.lstat(fullpath)
            mtime = datetime.datetime.utcfromtimestamp(fstat.st_mtime)

            if 'sha1sum_time' in trec:
                timediff =  (mtime - trec['sha1sum_time']).total_seconds()
            else:
                timediff = 1e12 #force recalculation

            # allow for at least half a second of leeway - at times
            # the difference between modification time and when the
            # system has taken the sha1sum does not have enough
            # resolution
            if timediff > 0.5:
                # might be modified - create a madfile object which will check
                # more thoroughly
                COUNTER['mod?'] += 1
                modfiles.append(fullpath)
                madfile = mad2.util.get_mad_file(app, fullpath)
                save_to_mongo(app, madfile)
            else:
                COUNTER['ok'] += 1

            # remove this file from the "must save" list - it's already
            # present
            if trec['filename'] in must_save_files:
                must_save_files.remove(trec['filename'])

        # save new files
        for filename in must_save_files:
            last_screen_update = screen_update(COUNTER, last_screen_update, 'new - ' + root)


            filename = os.path.join(root, filename)
            remove_dir = False # again - stuff here - do not remove
            filestat = os.lstat(filename)
            if filestat.st_size < args.min_file_size:
                continue
            COUNTER['new'] += 1
            newfiles.append(filename)
            madfile = mad2.util.get_mad_file(app, filename)
            save_to_mongo(app, madfile)

        if not remove_dir:
            if root in dirs_to_delete:
                dirs_to_delete.remove(root)

        mongo_flush(app)

    if len(dirs_to_delete) > 0:
        lg.info("lastly: removing records from %d dirs", len(dirs_to_delete))

    for dirname in dirs_to_delete:
        #hmm - skipping the flush step - directly removing here...
        COUNTER['dir_rm'] += 1
        transient_db.remove(
            { "dirname": dirname,
              "host": socket.gethostname(), })


    mongo_flush(app)

    for k, v in COUNTER.items():
        lg.warning("%10s: %d", k, v)
    if len(modfiles) > 0:
        lg.warning("Modified files: (last 5)")
        for mf in modfiles:
            lg.warning(" - %s", mf)
    if len(newfiles) > 0:
        lg.warning("New files: (last 5)")
        for mf in newfiles:
            lg.warning(" - %s", mf)

@leip.hook("finish")
def save_to_mongo_finish(app):
    mongo_flush(app)


@leip.hook("madfile_post_load")
def add_hook(app, madfile):
    madfile.all['_id_transient'] = get_mongo_transient_id(madfile)


@leip.hook("madfile_save")
def store_in_mongodb(app, madfile):
    lg.debug("running store_in_mongodb")
    save_to_mongo(app, madfile)


@leip.subparser
def mongo(app, args):
    """
    Mongodb backend
    """
    pass  # this function is never called - it's just a placeholder


@leip.arg('file', nargs="*")
@leip.subcommand(mongo, "show")
def mongo_show(app, args):
    """
    Show mongodb records
    """
    transient_db = get_mongo_transient_db(app)
    for madfile in get_all_mad_files(app, args):
        mongo_id = madfile['uuid']
        if mongo_id:
            print('#', mongo_id, madfile['filename'])
            rec = transient_db.find_one({'_id': mongo_id})
            if not rec:
                continue
            for key in rec:
                if key == '_id':
                    print('uuid\t{1}\t{2}'.format(key, rec[key]))
                    continue
                print('{0}\t{1}'.format(key, rec[key]))


@leip.flag('-c', '--core')
@leip.arg('mongo_id')
@leip.subcommand(mongo, "get")
def mongo_get(app, args):
    """
    get a mongodb record based on id
    """

    if args.core:
        collection = get_mongo_core_db(app)
    else:
        collection = get_mongo_transient_db(app)

    rec = collection.find_one({'_id': args.mongo_id[:24]})
    if not rec:
        return

    print(yaml.safe_dump(rec, default_flow_style=False))


@leip.flag('-c', '--core')
@leip.arg('mongo_id')
@leip.subcommand(mongo, "del")
def mongo_del(app, args):
    """
    get a mongodb record based on id
    """
    if args.core:
        MONGO = get_mongo_core_db(app)
    else:
        MONGO = get_mongo_transient_db(app)

    mongo_id = args.mongo_id
    MONGO.remove({'_id': mongo_id})


@leip.hook("madfile_delete")
def transient_delete(app, madfile):
    transient_id = madfile.get('_id_transient')
    MONGO = get_mongo_transient_db(app)
    lg.debug("Deleting %s (%s)", madfile['inputfile'], transient_id)
    MONGO.remove({'_id': transient_id})


@leip.subcommand(mongo, "count")
def mongo_count(app, args):
    """
    Show the associated mongodb record
    """
    MONGO_mad = get_mongo_transient_db(app)
    print(MONGO_mad.count())


@leip.arg('-n', '--no', type=int, default=10)
@leip.subcommand(mongo, "last")
def mongo_last(app, args):
    MONGO_mad = get_mongo_transient_db(app)
    res = MONGO_mad.aggregate([
        {"$sort": {"save_time": -1}},
        {"$limit": args.no},
    ])
    for i, r in enumerate(res['result']):
        if i > args.no:
            break
        print("\t".join(
            [arrow.get(r['save_time']).humanize(),
              r['filename'], r.get('_id', '')]))


@leip.flag('--delete')
@leip.arg('-u', '--username')
@leip.arg('-b', '--backup')
@leip.arg('-D', '--dirname')
@leip.arg('-B', '--ignore_backup_volumes')
@leip.arg('-v', '--volume')
@leip.arg('-c', '--category')
@leip.arg('-p', '--project')
@leip.arg('-P', '--pi')
@leip.arg('-e', '--experiment')
@leip.arg('-f', '--filename')
@leip.arg('-H', '--host')
@leip.arg('-s', '--sha1sum')
@leip.arg('-z', '--min_filesize')
@leip.arg('-Z', '--max_filesize')
@leip.arg('-o', '--atime_older_than')
@leip.arg('-S', '--sort', help='sort on this field')
@leip.arg('-R', '--reverse_sort', help='reverse sort on this field')
@leip.arg('-l', '--limit', default=-1, type=int)
@leip.arg('-F', '--format', help='output format', default='{fullpath}')
@leip.flag('-r', '--raw', help='output raw YAML')
@leip.flag('--tsv', help='tab delimited output, --format is now interpreted '
          'as a comma separated list of fields to export')
@leip.command
def search(app, args):
    """
    Find files
    """

    MONGO_mad = get_mongo_transient_db(app)

    query = {}

    for f in ['username', 'backup', 'volume', 'host', 'dirname',
              'sha1sum', 'project', 'project', 'pi', 'category',
              'filename']:

        v = getattr(args, f)
        if v is None:
            continue
        elif v == '(none)':
            query[f] = { "$exists": False }
        elif v.startswith('/') and v.endswith('/'):
            rrr = re.compile(v[1:-1])
            query[f] = rrr
        else:
            query[f] = v

    if args.min_filesize:
        query['filesize'] = {"$gt": mad2.util.interpret_humansize(args.min_filesize)}

    if args.max_filesize:
        nq = query.get('filesize', {})
        nq["$lt"] = mad2.util.interpret_humansize(args.max_filesize)
        query['filesize'] = nq

    if args.atime_older_than:
        delta = datetime.timedelta(seconds=pytimeparse.parse(args.atime_older_than))
        cutoffdate = datetime.datetime.utcnow() - delta
        query['atime'] = {"$lte": cutoffdate}


    if args.delete:
        MONGO_mad.remove(query)
        return

    res = MONGO_mad.find(query)


    if args.sort:
        res = res.sort(args.sort, pymongo.ASCENDING)
    elif args.reverse_sort:
        res = res.sort(args.reverse_sort, pymongo.DESCENDING)

    if args.limit > 0:
        res = res.limit(args.limit)

    if args.tsv:
        if args.format == '{fullpath}':
            fields = 'host fullpath filesize category'.split()
        else:
            fields = args.format.split(',')
        for r in res:
            vals = [r.get(x, 'n.a.') for x in fields]
            print("\t".join(map(str, vals)))
    elif args.raw:
        print(yaml.safe_dump(list(res), default_flow_style=False))
    else:
        #ensure tab characters
        format = args.format.replace(r'\t', '\t')
        for r in res:
            while True:
                try:
                    print(format.format(**r))  # 'fullpath'])
                except KeyError as e:
                    r[e.args[0]] = '(no value)'
                    continue
                break


@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'sum'),
                  'group_by', 60 * 60 * 24)
def _single_sum(app, group_by=None, force=False):
    groupby_field = "${}".format(group_by)
    MONGO_mad = get_mongo_transient_db(app)

    res = MONGO_mad.aggregate([
        {"$match": {"orphan": False}},
        {'$group': {
            "_id": groupby_field,
            "total": {"$sum": "$filesize"},
            "count": {"$sum": 1}}},
        {"$sort": {"total": -1
                   }}])

    return list(res)


@leip.flag('-f', '--force', help='force query (otherwise use cache, and'
           + ' query only once per day')
@leip.flag('-H', '--human', help='human readable')
@leip.arg('group_by', nargs='?', default='host')
@leip.subcommand(mongo, "sum")
def mongo_sum(app, args):
    """
    Show the associated mongodb record
    """

    res = _single_sum(app, group_by=args.group_by, force=args.force)
    total_size = int(0)
    total_count = 0

    mgn = len("Total")
    for reshost in res:
        gid = reshost['_id']
        if gid is None:
            mgn = max(4, mgn)
        else:
            mgn = max(len(str(reshost['_id'])), mgn)

    fms = "{:" + str(mgn) + "}\t{:>10}\t{:>9}"
    for reshost in res:
        total = reshost['total']
        count = reshost['count']
        total_size += int(total)
        total_count += count
        if args.human:
            total_human = humansize(total)
            categ = reshost['_id']
            if categ is None:
                categ = "<undefined>"
            print(fms.format(
                categ, total_human, count))
        else:
            print("{}\t{}\t{}".format(
                reshost['_id'], total, count))

    if args.human:
        total_size_human = humansize(total_size)
        print(fms.format(
            "Total", total_size_human, count))
    else:
        print("Total\t{}\t{}".format(total_size, total_count))


@leip.flag('-H', '--human', help='human readable')
@leip.flag('-s', '--sort_on_field')
@leip.arg('group_by_2')
@leip.arg('group_by_1')
@leip.subcommand(mongo, "sum2")
def mongo_sum2(app, args):
    """
    Show the associated mongodb record
    """
    gb1_field = "${}".format(args.group_by_1)
    gb2_field = "${}".format(args.group_by_2)

    # gb_pair_field = "${}_${}".format(gb1_field, gb2_field)

    MONGO_mad = get_mongo_transient_db(app)

    if args.sort_on_field:
        sort_field = '_id'
        sort_order = 1
    else:
        sort_field = 'total'
        sort_order = -1

    query = [
        {"$match": {"orphan": False}},
        {'$group': {
            "_id": {
                "group1": gb1_field,
                "group2": gb2_field},
            "total": {"$sum": "$filesize"},
            "count": {"$sum": 1}}},
        {"$sort": {
            "sort_field": sort_order
        }}]

    res = list(MONGO_mad.aggregate(query))
    total_size = 0
    total_count = 0


    gl1 = gl2 = len("Total")

    for r in res:
        g1 = str(r['_id'].get('group1'))
        g2 = str(r['_id'].get('group2'))
        gl1 = max(gl1, len(g1))
        gl2 = max(gl2, len(g2))

    fms = "{:" + str(gl1) + "}  {:" + str(gl2) + "}  {:>10}  {:>9}"
    for r in res:
        g1 = str(r['_id'].get('group1', '-'))
        g2 = str(r['_id'].get('group2', '-'))
        total = r['total']
        count = r['count']
        total_size += total
        total_count += count
        if args.human:
            total = humansize(total)
            print(fms.format(g1, g2, total, count))
        else:
            print("{}\t{}\t{}\t{}".format(g1, g2, total, count))

    if args.human:
        total_size = humansize(total_size)
        print(fms.format(
            "Total", "", total, count))
    else:
        print("Total\t\t{}\t{}".format(total_size, total_count))


@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'sum'),
                  'name', 60 * 60 * 23)
def _complex_sum(app, name, fields=['username', 'host'],
                 force=False):

    MONGO_mad = get_mongo_transient_db(app)
    qid = dict([(x, "$" + x) for x in fields])
    aggp = [{'$group': {
        "_id": qid,
        "total": {"$sum": "$filesize"},
        "count": {"$sum": 1}}}]
    res = MONGO_mad.aggregate(aggp)
    return list(res)



@leip.flag('-f', '--force', help='force query (otherwise use cache, and'
           + ' query only once per day')
@leip.arg('output_file', help='{stamp} will be replaced by a timestamp')
@leip.subcommand(mongo, "csum")
def complex_sum(app, args):
    """
    Query on a number of fields
    """

    fields = [
        'username',
        'experiment_type',
        'host',
        'volume',
        'orphan',
        'filetype',
        'organism',
        'biologist',
        'genome_build',
        'experiment_type',
        'pi',
        'project',
        'category',
        'backup']

    res = _complex_sum(app, name='table_query',
                       fields=fields, force=args.force)

    utc = arrow.now()
    stamp = utc.format('YYYY-MM-DD')

    # data = collections.defaultdict(list)
    precords = []
    for i, rec in enumerate(res):
        _id = rec['_id']
        prec = {}
        prec['count'] = rec['count']
        prec['sum'] = rec['total']
        for f in fields:
            prec[f] = _id.get(f)
        precords.append(prec)

    out = args.output_file.format(stamp=stamp)

    #HACK: for whatever reason yaml does not want to output bson/int64's
    #so - now I make sure they're all int :(
    for r in precords:
        r['sum'] = int(r['sum'])

    with open(out, 'w') as F:
        F.write(yaml.safe_dump(precords, default_flow_style=False))

    #store in mongodb
    dbrec = dict(data=precords,
                 time = datetime.datetime.utcnow())

    db = get_mongo_db(app)
    csum_coll = db['csum']
    csum_coll.insert_one(dbrec)
#    d =


def get_latest_csum(app, args):

    import pandas as pd

    # elect the correct csum file
    csum_path = app.conf['plugin.mongo.csum_path']
    assert '{stamp}' in csum_path
    csum_glob = csum_path.replace('{stamp}', '*')
    csums = []
    css = csum_path.index('{stamp}')
    for f in glob.glob(csum_glob):
        cdate = f[css:].split('.')[0]
        csums.append((cdate, f))
    csums.sort()
    cdate, csum_file = csums[-1]
    lg.info("Using csum file: %s", csum_file)
    lg.info("From date: %s", cdate)

    pickle_file = csum_file.replace('.yaml', '') + '.pickle'
    if os.path.exists(pickle_file):
        data = pd.read_pickle(pickle_file)
        return data

    with open(csum_file) as F:
        data = yaml.load(F)

    data = pd.DataFrame(data)
    data.fillna('n.a.', inplace=True)

    data.to_pickle(pickle_file)
    return data


@leip.arg("-s", "--select", nargs=2, action='append')
@leip.arg("group", nargs='+')
@leip.subcommand(mongo, "csum_group")
def mongo_csum_report(app, args):

    import pandas as pd

    pd.set_option('display.max_rows', None)

    data = get_latest_csum(app, args)

    if args.select is not None:
        for selkey, selval in args.select:
            data = data[data[selkey] == selval]

    grod = data.groupby(args.group)[['sum', 'count']].sum()
    grod.sort('sum', inplace=True, ascending=False)
    grod['sum'] = grod['sum'].apply(humansize)
    print(grod)


@leip.arg("-s", "--select", nargs=2, action='append')
@leip.arg('values', nargs='*', default='sum')
@leip.arg("columns")
@leip.arg("index")
@leip.subcommand(mongo, "csum_pivot")
def mongo_csum_pivot(app, args):

    import pandas as pd
    pd.set_option('display.max_rows', None)

    data = get_latest_csum(app, args)
    print(data.head())

    if args.select is not None:
        for selkey, selval in args.select:
            data = data[data[selkey] == selval]

    piv = data.pivot_table(index=args.index, columns=args.columns,
                           values=args.values)
    piv = piv.applymap(humansize)
    piv.fillna("", inplace=True)
    print(piv)


@leip.flag('--remove-from-core', help='also remove from the core db')
@leip.arg('file', nargs="*")
@leip.command
def forget(app, args):
    MONGO = get_mongo_transient_db(app)
    MONGO_CORE = get_mongo_core_db(app)
    to_remove = []
    to_remove_core = []

    def go(coll, lst):
        coll.remove({'_id': {'$in': lst}})

    for madfile in get_all_mad_files(app, args):
        to_remove.append(madfile['_id_transient'])
        if args.remove_from_core:
            to_remove_core.append(['_id'])

        if len(to_remove) > 100:
            go(MONGO, to_remove)
            to_remove = []
        if len(to_remove_core) > 100:
            go(MONGO_CORE, to_remove_core)
            to_remove_core = []

    go(MONGO, to_remove)
    go(MONGO_CORE, to_remove_core)


@leip.flag('-f', '--force')
@leip.subcommand(mongo, "drop")
def mongo_drop(app, args):
    """
    Show the associated mongodb record
    """
    if not args.force:
        print("use --force to really drop the database")
        exit()

    MONGO_mad = get_mongo_transient_db(app)
    MONGO_mad.drop()


@leip.flag('-e', '--echo')
@leip.arg('file', nargs="*")
@leip.subcommand(mongo, "save")
def mongo_save(app, args):
    """
    Save to mongodb
    """
    for madfile in get_all_mad_files(app, args):
        lg.debug("save to mongodb: %s", madfile['inputfile'])
        save_to_mongo(app, madfile)
        if args.echo:
            print(madfile['inputfile'])


@leip.subcommand(mongo, "create_index")
def mongo_index(app, args):
    """
    Ensure indexes on the relevant fields
    """
    MONGO_transient = get_mongo_transient_db(app)
    MONGO_core = get_mongo_core_db(app)
    MONGO_transact, MONGO_sha1sum2transact = get_mongo_transact_db(app)

    core_index =app.conf['plugin.mongo.indici.core']
    transient_index =app.conf['plugin.mongo.indici.transient']
    transact_index =app.conf['plugin.mongo.indici.transact']
    sha2tra_index =app.conf['plugin.mongo.indici.sha1sum2transact']

    for db, flds in [(MONGO_transient, transient_index),
                     (MONGO_core, core_index),
                     (MONGO_transact, transact_index),
                     (MONGO_sha1sum2transact, sha2tra_index)]:
        for k, v in list(flds.items()):
            print(db, k, v)
            assert v==1
            db.ensure_index(k)


@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'keys'),
                  1, 60 * 60 * 24)
def _get_mongo_keys(app, collection, force=False):
    from bson.code import Code
    mapper = Code("""
        function() {
            for (var key in this) { emit(key, 1); }
        } """)

    rv = {}

    reducer = Code("function(key, vals) { return Array.sum(vals); }")
    if collection == 'transient':
        message("Get keys from the transient db")
        COLLECTION = get_mongo_transient_db(app)
    else:
        message("Get keys from the core db")
        COLLECTION = get_mongo_core_db(app)

    res = COLLECTION.map_reduce(mapper, reducer, "my_collection" + "_keys")

    for r in res.find():
        rv[r['_id']] = int(r['value'])

    return rv


@leip.flag('-f', '--force')
@leip.arg('output_file', help='{stamp} will be replaced by a timestamp')
@leip.subcommand(mongo, 'keys')
def mongo_keys(app, args):
    transient = _get_mongo_keys(app, 'transient', force=args.force)
    core = _get_mongo_keys(app, 'core', force=args.force)
    all_keys = list(sorted(set(transient.keys()) | set(core.keys())))

    max_key_len = max([len(x) for x in all_keys])

    utc = arrow.now()
    stamp = utc.format('YYYY-MM-DD')

    #print(('%-' + str(max_key_len) + 's: %12s %12s') % (
    #    '# ey', 'transient', 'core'))

    records = []

    for k in all_keys:
        records.append({
            'key': k,
            'transient': transient.get(k),
            'core': core.get(k)})

        #print(('%-' + str(max_key_len) + 's: %12s %12s') % (
        #    k, transient.get(k, ''), core.get(k, '')))

    out = args.output_file.format(stamp=stamp)
    with open(out, 'w') as F:
        F.write(yaml.safe_dump(records, default_flow_style=False))


@leip.arg('keyname', metavar='key')
@leip.command
def distinct(app, args):
    transient = get_mongo_transient_db(app)
    vals = sorted(transient.distinct(args.keyname))
    if None in vals:
        cprint('<None>', 'yellow')
    for v in vals:
        if v is None:
            continue
        cprint('"' + v + '"', 'green')


@leip.flag('--I-am-sure')
@leip.arg('key')
@leip.arg('database', choices=['core', 'transient'])
@leip.subcommand(mongo, 'remove_key_from_db')
def mongo_remove_key(app, args):
    lg.warning("removing %s from the %s db", args.key, args.database)
    if args.database == 'core':
        COLLECTION = get_mongo_core_db(app)
    elif args.databse == 'transient':
        COLLECTION = get_mongo_transient_db(app)

    print(COLLECTION)
    query = {args.key: {'$exists': True}}
    update = {"$unset": {args.key: ""}}
    print(query)
    print(update)
    COLLECTION.update(query, update, multi=True)


@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'command'),
                  1,  1)
def _run_mongo_command(app, name, collection, query, kwargs={}, force=False):
    """
    Execute mongo command.
    """
    MONGO_mad = get_mongo_transient_db(app)
    res = MONGO_mad.database.command(query, collection, **kwargs)
    return res


WASTE_PIPELINE = [
    {"$match": {"orphan": False}},
    {"$sort": {"sha1sum": 1}},
    {"$project": {"filesize": 1,
                  "sha1sum": 1,
                  "usage": {"$divide": ["$filesize", "$nlink"]}}},
    {"$group": {"_id": "$sha1sum",
                "no_records": {"$sum": 1},
                "mean_usage": {"$avg": "$usage"},
                "total_usage": {"$sum": "$usage"},
                "filesize": {"$max": "$filesize"}}},
    {"$project": {"filesize": 1,
                  "sha1sum": 1,
                  "total_usage": 1,
                  "waste": {"$subtract": ["$total_usage", "$filesize"]}}},
    {"$match": {"waste": {"$gt": 500}}},
    {"$group": {"_id": None,
                "no_files": {"$sum": 1},
                "waste": {"$sum": "$waste"}}}]


@leip.arg('-v', '--volume')
@leip.arg('-p', '--path_fragment')
@leip.flag('-e', '--echo', help='echo files for which >1 file is found'
           + '(taking -v & -p into account)')
@leip.arg('file', nargs="*")
@leip.flag('-r', '--raw_output')
@leip.command
def repl(app, args):
    """
    Save to mongodb
    """

    MONGO_mad = get_mongo_transient_db(app)

    backup_hosts = set()
    for host in app.conf['host']:
        if app.conf['host'][host]['backup']:
            backup_hosts.add(host)

    check_shasums = False
    if len(args.file) > 0:
        for f in args.file:
            if os.path.exists(f):
                break
            if len(f) != 40:
                break
        else:
            check_shasums = True

    def _process_query(query, madfile_in):
        res = MONGO_mad.find(query)
        for r in res:
            if args.volume and \
               r['volume'] != args.volume:
                continue

            if args.path_fragment and \
               args.path_fragment not in r['fullpath']:
                continue

            if args.echo:
                print(madfile_in['inputfile'])
                break

            days = (arrow.now() - arrow.get(r['save_time'])).days
            symlink = r.get('is_symlink', False)
            if symlink:
                stag = 'S'
            else:
                stag = '.'
            if args.raw_output:
                print("\t".join(map(str, [
                    r['nlink'], stag, (arrow.now() -
                                       arrow.get(r['save_time'])),
                    r['filesize'], r['host'], r['fullpath']
                ])))
            else:
                cprint('%1d%s' % (r['nlink'], stag), 'yellow', end=" ")
                cprint('%3d' % days, 'green', end="d ")
                cprint('%6s' % humansize(r['filesize']), 'white', end=" ")
                if r['host'] in backup_hosts:
                    cprint(r['host'], 'green', attrs=['bold'], end=':')
                else:
                    cprint(r['host'], 'cyan', end=':')
                cprint(r['fullpath'])

    if check_shasums:
        for sha1sum in args.file:
            query = {'sha1sum': sha1sum}
            _process_query(query, None)
    else:
        for madfile in get_all_mad_files(app, args):
            query = {'sha1sum': madfile['sha1sum']}
            _process_query(query, madfile)


FIND_WASTER_PIPELINE = [
    {"$match": {"orphan": False}},
    {"$project": {"filesize": 1,
                  "sha1sum": 1,
                  "usage": {"$divide": ["$filesize", "$nlink"]}}},
    {"$group": {"_id": "$sha1sum",
                "no_records": {"$sum": 1},
                "mean_usage": {"$avg": "$usage"},
                "total_usage": {"$sum": "$usage"},
                "filesize": {"$max": "$filesize"}}},
    {"$project": {"filesize": 1,
                  "total_usage": 1,
                  "waste": {"$subtract": ["$total_usage", "$filesize"]}}},
    {"$match": {"waste": {"$gt": 500}}},
    {"$sort": {"waste": -1}},
    {"$limit": 100}]


@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'waste'), 1,  24*60*60)
def _run_waste_command(app, name, force=False):
    """
    Execute mongo command.
    """
    MONGO_mad = get_mongo_transient_db(app)
    res = MONGO_mad.aggregate(FIND_WASTER_PIPELINE, allowDiskUse=True)
    return list(res)


@leip.flag('-N', '--no-color', help='no ansi coloring of output')
@leip.flag('--todb', help='save to mongo')
@leip.arg('-n', '--no-records', default=20, type=int)
@leip.flag('-f', '--force')
@leip.command
def waste(app, args):

    db = get_mongo_transient_db(app)

    res = _run_waste_command(app, 'waste_pipeline',
                             force=args.force)


    if args.todb:
        dbrec = {'time': datetime.datetime.utcnow(),
                 'data': res}
        db = mad2.util.get_mongo_db(app)
        db.waste.insert_one(dbrec)
        return

    def cprint_nocolor(*args, **kwargs):
        if 'color' in kwargs:
            del kwargs['color']
        if len(args) > 1:
            args = args[:1]
        print(*args, **kwargs)

    if args.no_color:
        cprint = cprint_nocolor
    for i, r in enumerate(res):
        if i >= args.no_records:
            break

        sha1sum = r['_id']
        if not sha1sum.strip():
            continue

        cprint(sha1sum, 'grey', end='')
        cprint(" sz ", "grey", end="")
        cprint("{:>9}".format(humansize(r['waste'])), end='')
        cprint(" w ", "grey", end="")
        cprint("{:>9}".format(humansize(r['filesize'])), end='')

        hostcount = collections.defaultdict(lambda: 0)
        hostsize = collections.defaultdict(lambda: 0)
        owners = set()
        for f in db.find({'sha1sum': sha1sum}):
            owners.add(f['username'])
            host = f['host']
            hostcount[host] += 1
            hostsize[host] += float(f['filesize']) / float(f['nlink'])

        for h in hostcount:
            print(' ', end='')
            cprint(h, 'green', end=':')
            cprint(hostcount[h], 'cyan', end="")

        cprint(" ", end="")
        cprint(", ".join(owners), 'red')



@leip.arg('-S', '--subject')
@leip.flag('-f', '--force')
@leip.command
def waste_text_report(app, args):

    db = get_mongo_transient_db(app)

    res = _run_waste_command(app, 'waste_pipeline',
                             force=args.force)['result']

    if args.subject:
        print("Subject: {}".format(args.subject))

    # his week's winner
    top = res[0]
    sha1sum = top['_id']
    owners = set()
    hostcount = collections.defaultdict(lambda: 0)
    hostsize = collections.defaultdict(lambda: 0)

    total = 0
    for rec in db.find({'sha1sum': sha1sum}):
        total += 1
        host = rec['host']
        hostcount[host] += 1
        hostsize[host] += float(rec['filesize']) / float(rec['nlink'])
        owners.add(rec['username'])

    print("This week's winner: {}".format(", ".join(owners)))
    print("One file, ", end="")
    print("{} location".format(total), end="")
    if total > 1:
        print("s", end="")
    print(", {} server,".format(len(hostcount)), end="")
    if len(hostcount) > 1:
        print("s", end="")
    print(" wasting {}.".format(humansize(top['waste'])))
    print("try:\n   mad repl {}\n\n".format(sha1sum))

    no_to_print = 20
    print("Waste overview: (no / sha1sum / waste / filesize)")
    print("=================================================\n")
    for i, r in enumerate(res):
        if i >= no_to_print:
            break

        sha1sum = r['_id']
        if not sha1sum.strip():
            continue
        print("{:2d} {} {:>10} {:>10}"
              .format(i, sha1sum, humansize(r['waste']),
                      humansize(r['filesize'])))

    print("\n\nDetails: (nlink/symlink/size/owner)")
    print("===================================")
    for i, r in enumerate(res):
        if i >= no_to_print:
            break

        sha1sum = r['_id']
        if not sha1sum.strip():
            continue

        print("# {:2d} {} {:>10} {:>10}"
              .format(i, sha1sum, humansize(r['waste']),
                      humansize(r['filesize'])))

        records = collections.defaultdict(list)
        hostcount = collections.defaultdict(lambda: 0)
        hostsize = collections.defaultdict(lambda: 0)

        for rec in db.find({'sha1sum': sha1sum}):
            host = rec['host']
            records[host].append(rec)
            hostcount[host] += 1
            hostsize[host] += float(rec['filesize']) / float(rec['nlink'])

        for h in hostcount:
            print("# Host: {}, copies: {}, total use: {}".format(
                h, hostcount[h], humansize(hostsize[h])))
            for rec in records[host]:
                smarker = '.'
                if rec.get('is_symlink'):
                    smarker = 'S'
                print("  {} {}".format(rec.get('nlink', '?'), smarker),
                      end=' ')
                print(humansize(rec['filesize']), end=' ')
                print(rec['username'])
                print("   " + rec['fullpath'])
#                for j, pp in enumerate(textwrap.wrap(rec['fullpath'], 70)):
#                    print(" " * 8 + pp)

        print("")
