from __future__ import print_function

import cPickle
import collections
import datetime
import glob
import logging
import os
import re
import time

import socket

import hashlib

import arrow
from pymongo import MongoClient
import pymongo
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

@leip.hook("flush") #can call this function as a hook!
def mongo_flush(app):

    global MONGO_REMOVE_CACHE
    global MONGO_SAVE_CACHE

    lg.debug("flush")
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

    lg.debug("prep for save: %s", madfile['inputfile'])
    if len(MONGO_SAVE_CACHE)  + len(MONGO_REMOVE_CACHE) > 20:
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

    if args.core:
         MONGO_C = get_mongo_core_db(app)
         core_id = rec['sha1sum'][:24]
         rec = MONGO_C.find_one({'_id': core_id})
    else:
        MONGO_D = get_mongo_db(app)
        mongo_id = args.mongo_id
        rec = MONGO_D.find_one({'_id': mongo_id})

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


@leip.hook("madfile_delete")
def dump_delete(app, madfile):
    dump_id = madfile.get('_id_dump')
    MONGO = get_mongo_db(app)
    lg.debug("Deleting %s (%s)", madfile['inputfile'], dump_id)
    MONGO.remove({'_id': dump_id})


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


@leip.flag('--delete')
@leip.arg('-u', '--username')
@leip.arg('-b', '--backup')
@leip.arg('-B', '--ignore_backup_volumes')
@leip.arg('-v', '--volume')
@leip.arg('-p', '--project')
@leip.arg('-P', '--pi')
@leip.arg('-e', '--experiment')
@leip.arg('-H', '--host')
@leip.arg('-s', '--sha1sum')
@leip.arg('-S', '--sort', help='sort on this field')
@leip.arg('-R', '--reverse_sort', help='reverse sort on this field')
@leip.arg('-l', '--limit', default=-1, type=int)
@leip.arg('-f', '--format', help='output format', default='{fullpath}')
@leip.command
def search(app, args):
    """
    Find files
    """

    MONGO_mad = get_mongo_db(app)

    query = {}

    for f in ['username', 'backup', 'volume', 'host',
              'sha1sum', 'project', 'project', 'pi']:
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

    if args.sort:
        res = res.sort(args.sort, pymongo.ASCENDING)
    elif args.reverse_sort:
        res = res.sort(args.reverse_sort, pymongo.DESCENDING)

    if args.limit > 0:
        res = res.limit(args.limit)
    
    for r in res:

        print(args.format.format(**r)) #['fullpath'])


@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'sum'),
                  'group_by', 60 * 60 * 24)
def _single_sum(app, group_by=None, force=False):
    groupby_field = "${}".format(group_by)
    MONGO_mad = get_mongo_db(app)

    res = MONGO_mad.aggregate([
        {"$match" : {"orphan" : False}},
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
            {"$match" : {"orphan" : False}},
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



@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'sum'),
                  'name', 60 * 60 * 23)
def _complex_sum(app, name, fields=['username', 'host'],
                 force=False):

    MONGO_mad = get_mongo_db(app)
    qid = dict([(x, "$" + x) for x in fields])
    aggp = [{'$group': {
                "_id": qid,
                "total": {"$sum": "$filesize"},
                "count": {"$sum": 1}}}]
    res = MONGO_mad.aggregate(aggp)
    return res['result']

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

    data = collections.defaultdict(list)
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
    with open(out, 'w') as F:
        F.write(yaml.safe_dump(precords, default_flow_style=False))


def get_latest_csum(app, args):

    import pandas as pd

    #select the correct csum file
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

    if not args.select is None:
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

    if not args.select is None:
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


@leip.flag('-r', '--remove-from-dump', help='remove from the dump db')
@leip.flag('--remove-from-core', help='also remove from the core db')
@leip.flag('--run', help='actually run - otherwise it\'s a dry run showing ' +
           'what would be deleted')
@leip.arg('dir', nargs="?", default='.')
@leip.flag('-e', '--echo', help='echo all files')
@leip.command
def forget_dir(app, args):
    args.forget_all = True
    flush_dir(app, args)


@leip.flag('-r', '--remove-from-dump', help='remove from the dump db '
           + 'otherwise files in dump are tagged as deleted')
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

    if args.remove_from_core and not args.remove_from_dump:
        lg.warning("This is an odd command to give - why remove from the core")
        lg.warning("  and not the dump db?")
        exit()

    MONGO_mad = get_mongo_db(app)
    MONGO_core = get_mongo_core_db(app)

    host = socket.gethostname()
    wd = os.path.abspath(os.getcwd())

    rex = re.compile("^" + wd)

    query = {
        'host' : host,
        'dirname' : rex,
        'orphan': False}

    ids_to_remove = []
    core_to_remove = []

    if args.run:
        pass

    res = MONGO_mad.find(query)
    for r in res:

        if os.path.exists(r['fullpath']):
            if args.echo:
                try:
                    print("+ " + r['fullpath'])
                except UnicodeEncodeError:
                    print("+ " + r['fullpath'].encode('utf8'))

            if not args.forget_all:
                continue
        else:
            try:
                print("- " + r['fullpath'])
            except UnicodeEncodeError:
                print("- " + r['fullpath'].encode('utf8'))

        ids_to_remove.append(r['_id'])

        if args.remove_from_core:
            core_to_remove.append(r['sha1sum'][:24])

        if args.run and len(ids_to_remove) >= 100:
            if args.remove_from_dump:
                lg.warning("removing %d records", len(ids_to_remove))
                MONGO_mad.remove( {'_id' : { '$in' : ids_to_remove } } )
            else:
                lg.warning("setting %d records to orphaned", len(ids_to_remove))

            ids_to_remove = []

        if args.run and args.remove_from_core and \
                len(core_to_remove) >= 200:
            lg.warning("removing %d records from core",
                       len(core_to_remove))
            MONGO_core.remove( {'_id' : { '$in' : core_to_remove } } )
            core_to_remove = []

    if args.run:
        if args.remove_from_dump:
            lg.warning("removing %d records", len(ids_to_remove))
            MONGO_mad.remove( {'_id' : { '$in' : ids_to_remove } } )
        else:
            lg.warning("setting %d records to orphaned", len(ids_to_remove))
            MONGO_mad.update(
                {'_id' : { '$in' : ids_to_remove } },
                {'$set': {'orphan': True}}
                )

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
    MONGO_core = get_mongo_core_db(app)

    for f in app.conf['plugin.mongo.indici']:
        print("create index on: {}".format(f))
        MONGO_mad.ensure_index(f)
        MONGO_core.ensure_index(f)


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
@leip.arg('output_file', help='{stamp} will be replaced by a timestamp')
@leip.subcommand(mongo, 'keys')
def mongo_keys(app, args):
    dump = _get_mongo_keys(app, 'dump', force=args.force)
    core = _get_mongo_keys(app, 'core', force=args.force)
    all_keys = list(sorted(set(dump.keys()) | set(core.keys())))

    max_key_len = max([len(x) for x in all_keys])

    utc = arrow.now()
    stamp = utc.format('YYYY-MM-DD')

    print( ('%-' + str(max_key_len) + 's: %12s %12s') % (
          '#key', 'dump', 'core'))

    records = []

    for k in all_keys:
        records.append({
            'key': k,
            'dump' : dump.get(k),
            'core': core.get(k) })

        print( ('%-' + str(max_key_len) + 's: %12s %12s') % (
              k, dump.get(k, ''), core.get(k, '')))

    out = args.output_file.format(stamp=stamp)
    with open(out, 'w') as F:
        F.write(yaml.safe_dump(records, default_flow_style=False))


@leip.arg('keyname', metavar='key')
@leip.command
def distinct(app, args):
    dump = get_mongo_db(app)
    vals = sorted(dump.distinct(args.keyname))
    if None in vals:
        cprint('<None>', 'yellow')
    for v in vals:
        if v is None:
            continue
        cprint('"' + v + '"', 'green')


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
    {"$match" : {"orphan" : False}},
    {"$sort": {"sha1sum": 1 }},
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


@leip.flag('-e', '--echo')
@leip.arg('file', nargs="*")
@leip.command
def repl(app, args):
    """
    Save to mongodb
    """

    MONGO_mad = get_mongo_db(app)

    backup_hosts = set()
    for host in app.conf['host']:
        if app.conf['host'][host]['backup']:
            backup_hosts.add(host)


    check_shasums = False
    if args.file > 0:
        for f in args.file:
            if os.path.exists(f): break
            if len(f) != 40: break
        else: check_shasums = True

    def _process_query(query):
        res = MONGO_mad.find(query)
        for r in res:
            days = (arrow.now() - arrow.get(r['save_time'])).days
            symlink = r.get('is_symlink', False)
            if symlink:
                stag = 'S'
            else:
                stag = '.'
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
            query = {'sha1sum' : sha1sum}
            _process_query(query)
    else:
        for madfile in get_all_mad_files(app, args):
            #print(madfile['sha1sum'])
            query = {'sha1sum': madfile['sha1sum']}
            _process_query(query)



FIND_WASTER_PIPELINE = [
    {"$match" : {"orphan" : False}},
    {"$project": {"filesize": 1,
                  "sha1sum": 1,
                  "usage": {"$divide": ["$filesize", "$nlink"]}}},
    {"$group": {"_id": "$sha1sum",
                "no_records": {"$sum": 1 },
                "mean_usage": {"$avg": "$usage" },
                "total_usage": {"$sum": "$usage" },
                "filesize": {"$max": "$filesize" }}},
    {"$project": {"filesize": 1,
                  "total_usage": 1,
                  "waste": {"$subtract": ["$total_usage", "$filesize"]}}},
    {"$match": {"waste": {"$gt": 500}}},
    {"$sort" : {"waste": -1} },
    {"$limit": 100}]


@persistent_cache(leip.get_cache_dir('mad2', 'mongo', 'waste'), 1,  24*60*60)
def _run_waste_command(app, name, force=False):
    """
    Execute mongo command.
    """
    MONGO_mad = get_mongo_db(app)
    res = MONGO_mad.aggregate(FIND_WASTER_PIPELINE, allowDiskUse=True)
    return res


@leip.flag('-N', '--no-color', help='no ansi coloring of output')
@leip.arg('-n', '--no-records', default=20, type=int)
@leip.flag('-f', '--force')
@leip.command
def waste(app, args):

    db = get_mongo_db(app)

    res = _run_waste_command(app, 'waste_pipeline',
                             force=args.force)['result']

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
        cprint("{:>9}".format(humansize(r['filesize'])),end='')

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

    db = get_mongo_db(app)

    res = _run_waste_command(app, 'waste_pipeline',
                             force=args.force)['result']


    if args.subject:
        print("Subject: {}".format(args.subject))


    #This week's winner
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
    if total > 1: print("s", end="")
    print(", {} server,".format(len(hostcount)), end="")
    if len(hostcount) > 1: print("s", end="")
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
        print("{:2d} {} {:>10} {:>10}"\
                    .format(i, sha1sum, humansize(r['waste']),
                            humansize(r['filesize'])))


    print("\n\nDetails: (nlink/symlink/size/owner)")
    print("===================================")
    import textwrap
    for i, r in enumerate(res):
        if i >= no_to_print:
            break

        sha1sum = r['_id']
        if not sha1sum.strip():
            continue

        print("# {:2d} {} {:>10} {:>10}"\
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
            print("## Host: {}, copies: {}, total use: {}".format(
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

