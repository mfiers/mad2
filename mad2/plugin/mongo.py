from __future__ import print_function

import logging
from pymongo import MongoClient
from bson.objectid import ObjectId
import leip
import copy
import datetime
import pprint
from mad2.util import get_all_mad_files


lg = logging.getLogger(__name__)
#lg.setLevel(logging.DEBUG)

def mongo_prep_mad(mf):
    d = mf.all.copy()
    d.update(mf.mad)
    d.sha1 = d.hash.sha1
    d.mtime = d.hash.mtime
    del d.mongo
    del d.hash
    return d

@leip.hook("madfile_save")
def store_in_mongodb(app, madfile):
    lg.debug("running store_in_mongodb")
    mng_mad = _get_mng_collection(app)
    if madfile.mad.mongo.id:
        mongoid = madfile.mad.mongo.id
        newrec = mongo_prep_mad(madfile)
        newrec['_id'] = ObjectId(mongoid)
        mng_mad.save(newrec)
    else:
        newrec = mongo_prep_mad(madfile)
        mongoid = mng_mad.insert(newrec)
        lg.debug("Updating mongo record {0}".format(mongoid))
        madfile.mad.mongo.id = str(mongoid)

def _get_mng_collection(app):
    host = app.conf.plugin.mongo.host
    port = app.conf.plugin.mongo.get('port', 27017)
    client = MongoClient(host, port)
    return client.mad.mad

@leip.arg('file', nargs="+")
@leip.command
def mongo_show(app, args):
    """
    Show the associated mongodb record
    """
    mng_mad = _get_mng_collection(app)
    for madfile in get_all_mad_files(app, args):
        if madfile.mad.mongo.id:
            print ('---', madfile.filename)
            mongoid = madfile.mad.mongo.id
            rec = mng_mad.find_one({'_id': ObjectId(mongoid)})
            #print(madfile.filename)
            if not rec:
                continue
            for key in rec:
                if key == 'mongo':
                    continue
                print('- {0}: {1}'.format(key, rec[key]))

@leip.command
def mongo_drop(app, args):
    """
    Show the associated mongodb record
    """
    mng_mad = _get_mng_collection(app)
    mng_mad.drop()

@leip.command
def mongo_index(app, args):
    """
    Ensure indexes on the relevant fields
    """
    mng_mad = _get_mng_collection(app)
    for f in app.conf.plugin.mongo.indici:
        mng_mad.ensure_index(f)



