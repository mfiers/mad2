from __future__ import print_function

import logging

from pymongo import MongoClient

from bson.objectid import ObjectId

import leip

from mad2.util import get_all_mad_files


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

    return mongo_id, d


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
    #MONGO_SAVE_CACHE.append(madfile)

    #if len(MONGO_SAVE_CACHE < 4):
    #    return

    #bulk = mng.initialize_ordered_bulk_op()

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
#            print('#', mongo_id, madfile['uuid'], madfile['filename'])
            rec = mng.find_one({'_id': mongo_id})
            # print(madfile.filename)
            if not rec:
                continue
            for key in rec:
                if key == '_id':
                    print('uuid\t{1}'.format(key, rec[key]))
                    continue
                print('{0}\t{1}'.format(key, rec[key]))


@leip.subcommand(mongo, "drop")
def mongo_drop(app, args):
    """
    Show the associated mongodb record
    """
    mng_mad = get_mng(app)
    mng_mad.drop()


@leip.arg('file', nargs="*")
@leip.subcommand(mongo, "save")
def mongo_save(app, args):
    """
    Save to mongodb
    """
    mng = get_mng(app)
    for madfile in get_all_mad_files(app, args):
        save_to_mongo(mng, madfile)


@leip.subcommand(mongo, "prepare")
def mongo_index(app, args):
    """
    Ensure indexes on the relevant fields
    """
    mng_mad = get_mng(app)
    for f in app.conf['plugin.mongo.indici']:
        print("create index on: {}".format(f))
        mng_mad.ensure_index(f)
