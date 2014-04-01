from __future__ import print_function

import datetime
import logging


from pymongo import MongoClient
from bson.objectid import ObjectId

import leip

from mad2.util import get_all_mad_files, humansize


lg = logging.getLogger(__name__)
# lg.setLevel(logging.DEBUG)

MONGO_SAVE_CACHE = []
MONGO_SAVE_COUNT = 0
MNG = None


def mongo_prep_mad(mf):
    d = dict(mf)
    mongo_id = mf['uuid']
    d['_id'] = mongo_id
    del d['uuid']

    sha1 = mf['hash']['sha1']
    if sha1:
        d['sha1'] = sha1

    d['save_time'] = datetime.datetime.utcnow()
    return mongo_id, d

# @leip.hook("madfile_orphan")
# def store_orphan(app, madfile):
#     mng = get_mng(app)
#     mng['orphan'] = True
#     save_to_mongo(mng, madfile)

@leip.hook("madfile_save")
def store_in_mongodb(app, madfile):
    lg.debug("running store_in_mongodb")
    mng = get_mng(app)
    save_to_mongo(mng, madfile)


# @leip.hook("finish")
# def finish_mongo_write(app, *args, **kwargs):
#     print("finish")


def save_to_mongo(mng, madfile):
    global MONGO_SAVE_COUNT
    global MONGO_SAVE_CACHE

    MONGO_SAVE_COUNT += 1
    # MONGO_SAVE_CACHE.append(madfile)

    # if len(MONGO_SAVE_CACHE < 4):
    #    return

    lg.debug("collection object {}".format(mng))
    mongo_id, newrec = mongo_prep_mad(madfile)
    #print("save", mongo_id, madfile['filename'])
    mng.update({'_id': mongo_id}, newrec, True)
    # if mongo_id:
    #     mng.save(newrec)
    # else:
    #     mongo_id = mng.insert(newrec)
    #     lg.debug("Updating mongo record {0}".format(mongo_id))
    #     madfile.mad['mongo_id'] = str(mongo_id)
    #     madfile.save()


def get_mng(app):
    """
    Get the collection object
    """
    global MNG

    if not MNG is None:
        return MNG

    mongo_info = app.conf['plugin.mongo']
    host = mongo_info.get('host', 'localhost')
    port = mongo_info.get('port', 27017)
    lg.debug("connect mongodb {}:{}".format(host, port))
    client = MongoClient(host, port)
    MNG = client.mad2.mad2
    return MNG


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
    mng = get_mng(app)
    for madfile in get_all_mad_files(app, args):
        mongo_id = madfile['uuid']
        if mongo_id:
            print('#', mongo_id, madfile['filename'])
# print('#', mongo_id, madfile['uuid'], madfile['filename'])
            rec = mng.find_one({'_id': mongo_id})
            # print(madfile.filename)
            if not rec:
                continue
            for key in rec:
                if key == '_id':
                    print('uuid\t{1}'.format(key, rec[key]))
                    continue
                print('{0}\t{1}'.format(key, rec[key]))


@leip.subcommand(mongo, "count")
def mongo_count(app, args):
    """
    Show the associated mongodb record
    """
    mng_mad = get_mng(app)
    print(mng_mad.count())


@leip.flag('-H', '--human', help='human readable')
@leip.arg('group_by', nargs='?', default='host')
@leip.subcommand(mongo, "sum")
def mongo_sum(app, args):
    """
    Show the associated mongodb record
    """
    groupby_field = "${}".format(args.group_by)
    mng_mad = get_mng(app)
    res = mng_mad.aggregate([
        {'$group': {
            "_id": groupby_field,
            "total": {"$sum": "$filesize"},
            "count": {"$sum": 1}}},
        {"$sort" : { "total": -1
                  }}
    ])
    total_size = 0
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
        total_size += total
        total_count += count
        if args.human:
            total = humansize(total)
            print(fms.format(
                reshost['_id'], total, count))
        else:
            print("{}\t{}\t{}".format(
                reshost['_id'], total, count))

    if args.human:
        total_size = humansize(total_size)
        print(fms.format(
            "Total", total, count))
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

    mng_mad = get_mng(app)
    res = mng_mad.aggregate([
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

    mng_mad = get_mng(app)
    mng_mad.drop()


@leip.flag('-e', '--echo')
@leip.arg('file', nargs="*")
@leip.subcommand(mongo, "save")
def mongo_save(app, args):
    """
    Save to mongodb
    """
    mng = get_mng(app)
    for madfile in get_all_mad_files(app, args):
        lg.debug("save to mongodb: %s", madfile['inputfile'])
        save_to_mongo(mng, madfile)
        if args.echo:
            print(madfile['inputfile'])


@leip.subcommand(mongo, "prepare")
def mongo_index(app, args):
    """
    Ensure indexes on the relevant fields
    """
    mng_mad = get_mng(app)
    for f in app.conf['plugin.mongo.indici']:
        print("create index on: {}".format(f))
        mng_mad.ensure_index(f)
