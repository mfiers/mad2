from __future__ import print_function

import datetime
import logging
import os

import hashlib

from pymongo import MongoClient
from bson.objectid import ObjectId

import arrow
import yaml

import leip

from mad2.util import get_all_mad_files, humansize


lg = logging.getLogger(__name__)

MONGO_SAVE_CACHE = []
MONGO_SAVE_COUNT = 0
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
    coll = info.get('collection', 'core')
    lg.debug("connect mongodb %s:%s/%s/%s", host, port, dbname, coll)
    client = MongoClient(host, port)

    MONGOCORE = client[dbname][coll]

    return MONGOCORE

def mongo_prep_mad(mf):

    d = dict(mf)

    sha1sum = hashlib.sha1()
    sha1sum.update(mf['sha1sum'])
    sha1sum.update(mf['host'])
    sha1sum.update(mf['fullpath'])

    mongo_id = sha1sum.hexdigest()[:24]
    d['_id'] = mongo_id
    if 'uuid' in d:
        del d['uuid']
    if 'hash' in d:
        del d['hash']
    d['save_time'] = datetime.datetime.utcnow()

    return mongo_id, d


def save_to_mongo(MONGO, madfile):
    global MONGO_SAVE_COUNT
    global MONGO_SAVE_CACHE

    MONGO_SAVE_COUNT += 1

    lg.debug("collection object {}".format(MONGO))
    mongo_id, newrec = mongo_prep_mad(madfile)

    MONGO.update({'_id': mongo_id}, newrec, True)


@leip.hook("madfile_save")
def store_in_mongodb(app, madfile):
    lg.debug("running store_in_mongodb")
    MONGO = get_mongo_db(app)
    save_to_mongo(MONGO, madfile)


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
    # for key in rec:
    #     print('{0}\t{1}'.format(key, rec[key]))

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



@leip.flag('-H', '--human', help='human readable')
@leip.arg('group_by', nargs='?', default='host')
@leip.subcommand(mongo, "sum")
def mongo_sum(app, args):
    """
    Show the associated mongodb record
    """
    groupby_field = "${}".format(args.group_by)
    MONGO_mad = get_mongo_db(app)
    res = MONGO_mad.aggregate([
        {'$group': {
            "_id": groupby_field,
            "total": {"$sum": "$filesize"},
            "count": {"$sum": 1}}},
        {"$sort" : { "total": -1
                  }}
    ])
    total_size = long(0)
    total_count = 0

    mgn = len("Total")
    for reshost in res['result']:
        gid = reshost['_id']
        if gid is None:
            mgn = max(4, mgn)
        else:
            mgn = max(len(reshost['_id']), mgn)

    fms = "{:" + str(mgn) + "}\t{:>10}\t{:>9}"
    for reshost in res['result']:
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
    res = MONGO_mad.aggregate([
            {'$group': {
                "_id": {
                    "group1": gb1_field,
                    "group2": gb2_field },
                "total": {"$sum": "$filesize"},
                "count": {"$sum": 1}}},
             {"$sort" : { "total": -1
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
    MONGO = get_mongo_db(app)
    for madfile in get_all_mad_files(app, args):
        lg.debug("save to mongodb: %s", madfile['inputfile'])
        save_to_mongo(MONGO, madfile)
        if args.echo:
            print(madfile['inputfile'])


@leip.subcommand(mongo, "prepare")
def mongo_index(app, args):
    """
    Ensure indexes on the relevant fields
    """
    MONGO_mad = get_mongo_db(app)
    for f in app.conf['plugin.mongo.indici']:
        print("create index on: {}".format(f))
        MONGO_mad.ensure_index(f)
