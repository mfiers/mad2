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

@leip.hook("madfile_save")
def store_in_mongodb(app, madfile):
    lg.debug("running store_in_mongodb")
    mng_mad, mng_hist = _get_mng_collection(app)
    if 'mongoid' in madfile.mad:
        mongoid = madfile.mad.mongoid
        hist_rec = mng_mad.find_one({'_id': ObjectId(mongoid)})
        #keep historical record
        del(hist_rec["_id"])
        hist_rec['mongoid'] = mongoid
        hist_rec['date'] = datetime.datetime.now()
        mng_hist.insert(hist_rec)
        #store record
        newrec = madfile.data()
        del(newrec['mongoid'])
        newrec['_id'] = ObjectId(mongoid)
        mng_mad.save(newrec)
    else:
        mongoid = mng_mad.insert(madfile.data())
        lg.debug("Updating mongo record {0}".format(mongoid))
        madfile.mad.mongoid = str(mongoid)

def _get_mng_collection(app):
    host = app.conf.plugin.mongo.host
    port = app.conf.plugin.mongo.get('port', 27017)
    client = MongoClient(host, port)
    return client.mad.mad, client.mad.history

@leip.arg('file', nargs="+")
@leip.command
def mongo_show(app, args):
    """
    Show the associated mongodb record
    """
    mng_mad, mng_hist = _get_mng_collection(app)
    for madfile in get_all_mad_files(app, args):
        if 'mongoid' in madfile.mad:
            mongoid = madfile.mad.mongoid
            rec = mng_mad.find_one({'_id': ObjectId(mongoid)})
            #print(madfile.filename)
            for key in rec:
                print('- {0}: {1}'.format(key, rec[key]))

