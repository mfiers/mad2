import logging
import os

import fantail

import datetime

from pymongo import MongoClient
from bson.objectid import ObjectId

import leip

import mad2.hash

lg = logging.getLogger(__name__)
lg.setLevel(logging.DEBUG)

MONGO_SAVE_CACHE = []
MONGO_SAVE_COUNT = 0
MNG = None


def mongo_prep_mad(mf):

    d = dict(mf)

    mongo_id = mf['sha1sum'][:24]
    d['_id'] = mongo_id

    mongo_id = mf['sha1sum'][:24]
    d['_id'] = mongo_id

    d['save_time'] = datetime.datetime.utcnow()
    return mongo_id, d


class MongoStore():

    def __init__(self, conf):
        lg.debug("starting mongostore")
        self.conf = conf
        self.host = self.conf.get('host', 'localhost')
        self.port = int(self.conf.get('port', 27017))
        self.client = MongoClient(self.host, self.port)
        self.db_name = self.conf.get('db', 'mad2')
        self.collection_name = self.conf.get('collection', 'core')
        self.db_core = self.client[self.db_name][self.collection_name]

    def prepare(self, madfile):
        if madfile.get('isdir', False):
            #no directories
            return
        #nothing to prepare -
        pass

    def save(self, madfile):
        """Save data to the mongo database"""

        if not 'sha1sum' in madfile:
            return

        mongo_id = madfile['sha1sum'][:24]
        core = dict(madfile.mad)
        full = dict(madfile)

        lg.debug("mongo save {}".format(madfile['inputfile']))
        lg.debug("mongo id {}".format(mongo_id))

        self.db_core.update({'_id': mongo_id}, core, True)
        #self.db_full.update({'_id': mongo_id}, full, True)


    def load(self, madfile):
        """
        Load the
        """

        if not 'sha1sum' in madfile:
            return

        hashfile = os.path.join(madfile['dirname'], 'SHA1SUMS')
        sha1 = mad2.hash.check_hashfile(hashfile, madfile['filename'])
        mongo_id = sha1[:24]
        lg.debug("getting mad data for {}".format(
                 madfile['inputfile']))
        lg.debug(" - sha1: {}".format(sha1))
        lg.debug(" - mongo_id: {}".format(mongo_id))
        data = self.db_core.find_one({'_id': mongo_id})
        madfile.mad.update(data)
