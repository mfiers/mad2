from __future__ import print_function

import cPickle
import datetime
import logging
import os
import re
import time

import socket

import hashlib

import arrow
from pymongo import MongoClient
from termcolor import cprint
import yaml

import leip

from mad2.util import get_all_mad_files, humansize, persistent_cache
from mad2.ui import message

lg = logging.getLogger(__name__)

MONGO_SAVE_CACHE = []
MONGO_SAVE_COUNT = 0
MONGO_REMOVE_COUNT = 0
MONGO = None
MONGOCORE = None


def get_mongo_db(app):
    """
    Get the collection object
    """
    global MONGO

    if not MONGO is None:
        return MONGO

    mongo_info = app.conf['plugin.mongo']
    host = mongo_info.get('host', 'localhost')
    port = mongo_info.get('port', 27017)
    dbname = mongo_info.get('db', 'mad2')
    coll = mongo_info.get('collection', 'mad2')
    lg.debug("connect mongodb {}:{}".format(host, port))
    client = MongoClient(host, port)

    MONGO = client[dbname][coll]

    return MONGO


def get_mongo_core_db(app):
    """
    Get the core collection object
    """
    global MONGOCORE

    if not MONGOCORE is None:
        return MONGOCORE

    info = app.conf['store.mongo']
    host = info.get('host', 'localhost')
    port = info.get('port', 27017)
    dbname = info.get('db', 'mad2')
    coll = info.get('collection', 'dump')
    lg.debug("connect mongodb %s:%s/%s/%s", host, port, dbname, coll)
    client = MongoClient(host, port)

    MONGOCORE = client[dbname][coll]

    return MONGOCORE


def get_mongo_dump_id(mf):
    sha1sum = hashlib.sha1()
    sha1sum.update(mf['host'])
    sha1sum.update(mf['fullpath'])
    return sha1sum.hexdigest()[:24]


def mongo_prep_mad(mf):

    mongo_id = get_mongo_dump_id(mf)

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

def mongo_flush(app):

    global MONGO_REMOVE_CACHE
    global MONGO_SAVE_CACHE

    if (len(MONGO_SAVE_CACHE) + len(MONGO_REMOVE_CACHE)) == 0:
        return

    collection = get_mongo_db(app)
    bulk = collection.initialize_unordered_bulk_op()

    for i, r in MONGO_SAVE_CACHE:
        bulk.find({'_id': i}).upsert().replace_one(r)
    res = bulk.execute()

    for i in MONGO_REMOVE_CACHE:
        #should try to do this in bulk, but uncertain how...
        lg.warning("removing id: %s", i)
        collection.remove({'_id': i})


    #print(res)
    lg.debug("Modified %d records", res['nModified'])
    MONGO_SAVE_CACHE = []
    MONGO_REMOVE_CACHE= []


def save_to_mongo(app, madfile):
    global MONGO_SAVE_COUNT
    global MONGO_SAVE_CACHE
    global MONGO_REMOVE_CACHE
    global MONGO_REMOVE_COUNT

    MONGO_SAVE_COUNT += 1

    mongo_id, newrec = mongo_prep_mad(madfile)

    if madfile['orphan']:
        MONGO_REMOVE_CACHE.append(mongo_id)
        lg.warning("removing %s from dump db", madfile['inputfile'])
    else:
        #print(newrec['host'])
        MONGO_SAVE_CACHE.append((mongo_id, newrec))

    if len(MONGO_SAVE_CACHE)  + len(MONGO_REMOVE_CACHE) > 33:
        mongo_flush(app)


@leip.hook("finish")
def save_to_mongo_finish(app):
    mongo_flush(app)

    #lg.critical('Finish')


@leip.hook("madfile_post_load")
def add_hook(app, madfile):
    madfile.mad['_id_dump'] = get_mongo_dump_id(madfile)


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
    MONGO = get_mongo_db(app)
    for madfile in get_all_mad_files(app, args):
        mongo_id = madfile['uuid']
        if mongo_id:
            print('#', mongo_id, madfile['filename'])
            rec = MONGO.find_one({'_id': mongo_id})
            # print(madfile.filename)
            if not rec:
                continue
            for key in rec:
                if key == '_id':
                    print('uuid\t{1}'.format(key, rec[key]))
                    continue
                print('{0}\t{1}'.format(key, rec[key]))


@leip.flag('-c', '--core')
@leip.arg('mongo_id')
@leip.subcommand(mongo, "get")
def mongo_get(app, args):
    """
    get a mongodb record based on id
    """
    MONGO_D = get_mongo_db(app)

    mongo_id = args.mongo_id

    rec = MONGO_D.find_one({'_id': mongo_id})

    if args.core:
         MONGO_C = get_mongo_core_db(app)
         core_id = rec['sha1sum'][:24]
         rec = MONGO_C.find_one({'_id': core_id})


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
        MONGO = get_mongo_db(app)

    mongo_id = args.mongo_id
    MONGO.remove({'_id': mongo_id})


@leip.subcommand(mongo, "count")
def mongo_count(app, args):
    """
    Show the associated mongodb record
    """
    MONGO_mad = get_mongo_db(app)
    print(MONGO_mad.count())


@leip.arg('-n', '--no', type=int, default=10)
@leip.subcommand(mongo, "last")
def mongo_last(app, args):
    now = arrow.now()
    MONGO_mad = get_mongo_db(app)
    res = MONGO_mad.aggregate([
        {"$sort" : { "save_time": -1 }},
        {"$limit" : args.no},
    ])
    for i, r in enumerate(res['result']):
        if i > args.no:
            break
        print("\t".join(
            [ arrow.get(r['save_time']).humanize(),
              r['filename'], r.get('_id', '')]))


@leip.flag('-e', '--echo')
@leip.arg('file', nargs="*")
@leip.command
def repl(app, args):
    """
    Save to mongodb
    """

    MONGO_mad = get_mongo_db(app)

    for madfile in get_all_mad_files(app, args):
        print(madfile['sha1sum'])
        query = {'sha1sum': madfile['sha1sum']}
        res = MONGO_mad.find(query)
        for r in res:
#            print(r)

            if r.get('is_backup_volume'):
                cprint('B', 'green', end='')
            else:
                cprint('O', 'yellow', end='')
            cprint('%1d' % r['nlink'], end="")
            cprint(" ", end="")
            cprint(r['host'], 'cyan', end=':')
            cprint(r['fullpath'], 'blue')


@leip.flag('--delete')
@leip.arg('-u', '--username')
@leip.arg('-b', '--backup')
@leip.arg('-B', '--ignore_backup_volumes')
@leip.arg('-v', '--volume')
@leip.arg('-H', '--host')
@leip.arg('-s', '--sha1sum')
@leip.command
def search(app, args):
    """
    Find files
    """

    MONGO_mad = get_mongo_db(app)

    query = {}

    for f in ['username', 'backup', 'volume', 'host',
              'sha1sum']:
        if not f in args:
            continue

        v = getattr(args, f)
        if v is None:
            continue
        query[f] = v

    if args.delete:
        MONGO_mad.remove(query)
        return

    res = MONGO_mad.find(query)
    for r in res:
        print(r['fullpath'])


@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'sum'),
                  'group_by', 60 * 60 * 24)
def _single_sum(app, group_by=None, force=False):
    groupby_field = "${}".format(group_by)
    MONGO_mad = get_mongo_db(app)

    res = MONGO_mad.aggregate([
        {'$group': {
            "_id": groupby_field,
            "total": {"$sum": "$filesize"},
            "count": {"$sum": 1}}},
        {"$sort" : { "total": -1
                  }}])

    return res['result']


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
    total_size = long(0)
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
        total_size += long(total)
        total_count += count
        if args.human:
            total_human = humansize(total)
            print(fms.format(
                reshost['_id'], total_human, count))
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

    gb_pair_field = "${}_${}".format(gb1_field, gb2_field)

    MONGO_mad = get_mongo_db(app)

    if args.sort_on_field:
        sort_field = '_id'
        sort_order = 1
    else:
        sort_field = 'total'
        sort_order = -1
    res = MONGO_mad.aggregate([
            {'$group': {
                "_id": {
                    "group1": gb1_field,
                    "group2": gb2_field },
                "total": {"$sum": "$filesize"},
                "count": {"$sum": 1}}},
             {"$sort" : {
              "sort_field": sort_order
              }}
        ])
    total_size = 0
    total_count = 0

    gl1 = gl2 = len("Total")

    for r in res['result']:
        #print(r)
        g1 = str(r['_id'].get('group1'))
        g2 = str(r['_id'].get('group2'))
        gl1 = max(gl1, len(g1))
        gl2 = max(gl2, len(g2))

    fms = "{:" + str(gl1) + "}  {:" + str(gl2) + "}  {:>10}  {:>9}"
    for r in res['result']:
        g1 = str(r['_id'].get('group1', '-'))
        g2 = str(r['_id'].get('group2', '-'))
        total = r['total']
        count = r['count']
        total_size += total
        total_count += count
        if args.human:
            total = humansize(total)
            print(fms.format(g1,g2, total, count))
        else:
            print("{}\t{}\t{}\t{}".format(g1, g2, total, count))

    if args.human:
        total_size = humansize(total_size)
        print(fms.format(
            "Total", "", total, count))
    else:
        print("Total\t\t{}\t{}".format(total_size, total_count))


@leip.flag('--remove-from-core', help='also remove from the core db')
@leip.arg('file', nargs="*")
@leip.command
def forget(app, args):
    MONGO = get_mongo_db(app)
    MONGO_CORE = get_mongo_core_db(app)
    to_remove = []
    to_remove_core = []

    def go(coll, lst):
        coll.remove( {'_id' : { '$in' : lst } } )

    for madfile in get_all_mad_files(app, args):
        to_remove.append(madfile['_id_dump'])
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


@leip.flag('--remove-from-core', help='also remove from the core db')
@leip.flag('--run', help='actually run - otherwise it\'s a dry run showing ' +
           'what would be deleted')
@leip.arg('dir', nargs="?", default='.')
@leip.flag('-e', '--echo', help='echo all files')
@leip.command
def forget_dir(app, args):
    args.forget_all = True
    flush_dir(app, args)


@leip.flag('--remove-from-core', help='also remove from the core db')
@leip.flag('--run', help='actually run - otherwise it\'s a dry run showing ' +
           'what would be deleted')
@leip.flag('--forget-all', help='forget all files in this directory ' +
           'and below')
@leip.flag('-e', '--echo', help='echo all files')
@leip.arg('dir', nargs='?', default='.')
@leip.command
def flush_dir(app, args):
    """
    Recursively flush deleted files from the dump db
    """

    MONGO_mad = get_mongo_db(app)
    MONGO_core = get_mongo_core_db(app)

    host = socket.gethostname()
    wd = os.path.abspath(os.getcwd())

    rex = re.compile("^" + wd)

    query = {
        'host' : host,
        'dirname' : rex}

    ids_to_remove = []
    core_to_remove = []

    if args.run:
        pass

    res = MONGO_mad.find(query)
    for r in res:

        if os.path.exists(r['fullpath']):
            if args.echo:
                print("+ " + r['fullpath'])
            if not args.forget_all:
                continue
        else:
            print("- " + r['fullpath'])

        ids_to_remove.append(r['_id'])
        if args.remove_from_core:
            core_to_remove.append(r['sha1sum'][:24])

        if args.run and len(ids_to_remove) >= 100:
            lg.warning("removing %d records", len(ids_to_remove))
            MONGO_mad.remove( {'_id' : { '$in' : ids_to_remove } } )
            ids_to_remove = []

        if args.run and args.remove_from_core and \
                len(core_to_remove) >= 200:
            lg.warning("removing %d records from core",
                       len(core_to_remove))
            MONGO_core.remove( {'_id' : { '$in' : core_to_remove } } )
            core_to_remove = []

    if args.run:
        lg.warning("removing %d records", len(ids_to_remove))
        MONGO_mad.remove( {'_id' : { '$in' : ids_to_remove } } )

    if args.run and args.remove_from_core:
            lg.warning("removing %d records from core",
                       len(core_to_remove))
            MONGO_core.remove( {'_id' : { '$in' : core_to_remove } } )

@leip.flag('-f', '--force')
@leip.subcommand(mongo, "drop")
def mongo_drop(app, args):
    """
    Show the associated mongodb record
    """
    if not args.force:
        print("use --force to really drop the database")
        exit()

    MONGO_mad = get_mongo_db(app)
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
    MONGO_mad = get_mongo_db(app)
    for f in app.conf['plugin.mongo.indici']:
        print("create index on: {}".format(f))
        MONGO_mad.ensure_index(f)

@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'keys'),
                  1, 60*60*24)
def _get_mongo_keys(app, collection, force=False):
    from bson.code import Code
    mapper = Code("""
        function() {
            for (var key in this) { emit(key, 1); }
        } """)

    rv = {}

    reducer = Code("function(key, vals) { return Array.sum(vals); }")
    if collection == 'dump':
        message("Get keys from the dump db")
        COLLECTION = get_mongo_db(app)
    else:
        message("Get keys from the core db")
        COLLECTION = get_mongo_core_db(app)

    res =  COLLECTION.map_reduce(mapper, reducer, "my_collection" + "_keys")

    for r in res.find():
        rv[r['_id']] = int(r['value'])

    return rv


@leip.flag('-f', '--force')
@leip.subcommand(mongo, 'keys')
def mongo_keys(app, args):
    dump = _get_mongo_keys(app, 'dump', force=args.force)
    core = _get_mongo_keys(app, 'core', force=args.force)
    all_keys = list(sorted(set(dump.keys()) | set(core.keys())))
    max_key_len = max([len(x) for x in all_keys])
    print( ('%-' + str(max_key_len) + 's: %12s %12s') % (
          '#key', 'dump', 'core'))

    for k in all_keys:
        print( ('%-' + str(max_key_len) + 's: %12s %12s') % (
              k, dump.get(k, ''), core.get(k, '')))


@leip.flag('--I-am-sure')
@leip.arg('key')
@leip.arg('database', choices=['core', 'dump'])
@leip.subcommand(mongo, 'remove_key_from_db')
def mongo_remove_key(app, args):
    lg.warning("removing %s from the %s db", args.key, args.database)
    if args.database == 'core':
        COLLECTION = get_mongo_core_db(app)
    elif args.databse == 'dump':
        COLLECTION = get_mongo_db(app)

    print(COLLECTION)
    query = {args.key : {'$exists': True}}
    update = {"$unset": { args.key: "" }}
    print(query)
    print(update)
    COLLECTION.update(query, update, multi=True)


@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'command'),
                  1,  1)
def _run_mongo_command(app, name, collection, query, kwargs={}, force=False):
    """
    Execute mongo command.
    """
    MONGO_mad = get_mongo_db(app)
    res = MONGO_mad.database.command(query, collection, **kwargs)

    return res


WASTE_PIPELINE = [
    {"$sort": {"sha1sum": 1 } },
    {"$project": {"filesize": 1,
                   "sha1sum": 1,
                   "usage": {"$divide": ["$filesize", "$nlink"]}}},
    {"$group": {"_id": "$sha1sum",
                 "no_records": {"$sum": 1 },
                 "mean_usage": {"$avg": "$usage" },
                 "total_usage": {"$sum": "$usage" },
                 "filesize": {"$max": "$filesize" }}},
     {"$project": {"filesize": 1,
                   "sha1sum": 1,
                   "total_usage": 1,
                   "waste": {"$subtract": ["$total_usage", "$filesize"]}}},
    {"$match": {"waste": {"$gt": 500}}},
    {"$group": {"_id": None,
                "no_files": {"$sum": 1 },
                "waste": {"$sum": "$waste" }}}]



@leip.flag('-f', '--force')
@leip.command
def waste(app, args):

    mongo_info = app.conf['plugin.mongo']
    coll = mongo_info.get('collection', 'mad2')

    print(WASTE_PIPELINE)
    # res = _run_mongo_command(app, 'waste',  coll, WASTE_QUERY)
    # print(res)
# get

# db.runCommand(
#   { "aggregate": "dump",
#     "pipeline": [
#     { "$sort": { "sha1sum": 1 } },
#     { "$project": { "filesize": 1,  "sha1sum": 1,
#                   "usage": { "$divide": [ "$filesize", "$nlink" ] } } },
#     { "$group": { "_id": "$sha1sum",
#                "no_records": { "$sum": NumberInt(1) },
#                "mean_usage": { "$avg": "$usage" },
#                "total_usage": { "$sum": "$usage" },
#                "filesize": { "$max": "$filesize" } } },
#      { "$project": { "filesize": 1, "sha1sum": 1, "total_usage": 1,
#                   "waste": {"$subtract": ["$total_usage", "$filesize"] } } },
#     { "$match": {"waste": {"$gt": 500} } },
#     { "$group": { "_id": null,
#                 "no_files": { "$sum": NumberInt(1) },
#                 "waste": { "$sum": "$waste" } } },
# ], "allowDiskUse": true }
# )

