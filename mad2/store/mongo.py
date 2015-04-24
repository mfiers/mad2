
import datetime
import logging

from pymongo import MongoClient

lg = logging.getLogger(__name__)
# g.setLevel(logging.DEBUG)


MONGO_SAVE_CACHE = []
MONGO_SAVE_COUNT = 0
MNG = None


# hese fields we do not want to see in the core database
FORBIDDEN = ['hash', 'uuid', '_id_dump', 'host', 'volume']


def mongo_prep_mad(mf):

    d = dict(mf)

    mongo_id = mf['sha1sum'][:24]
    d['_id'] = mongo_id
    d['sha1sum'] = mf['sha1sum']
    d['save_time'] = datetime.datetime.utcnow()

    for f in FORBIDDEN:
        if f in d:
            del d[f]
    return mongo_id, d


class MongoStore():

    def __init__(self, conf):
        lg.debug("starting mongostore")
        self.conf = conf
        self.host = self.conf.get('host', 'localhost')
        self.port = int(self.conf.get('port', 27017))
        self.client = MongoClient(self.host, self.port)
        self.db_name = self.conf.get('db', 'mad2')
        self.corename = self.conf.get('collection', 'core')

        self.dumpname = self.conf.get('dump_collection', 'dump')

        self.db_core = self.client[self.db_name][self.corename]
        self.db_dump = self.client[self.db_name][self.dumpname]

        self.save_cache = []

    def prepare(self, madfile):
        return

    def changed(self, madfile):
        pass

    def delete(self, madfile):
        """
        We do not really delete a file here - but - this needs to be
        calleable
        """
        lg.debug("madfile delete %s", madfile['inputfile'])

    def save(self, madfile):
        """Save data to the mongo database"""

        if madfile['orphan']:
            lg.debug("Will not save orphan file")
            return

        if not madfile.get('sha1sum'):
            lg.warning("cannot save to mongodb without a sha1sum")
            return


        mongo_id = madfile['sha1sum'][:24]

        core = dict(madfile.mad)
        core['sha1sum'] = madfile['sha1sum']

        core['_id'] = madfile['_id_core']
        core['sha1sum'] = madfile['sha1sum']

        for to_remove in ['core', '_id_transient', '_id_dump', 'uuid',
                          '_id_core']:
            if to_remove in core:
                del core[to_remove]

        lg.debug("saving to id %s", mongo_id)
        self.save_cache.append((mongo_id, core))

        if len(self.save_cache) > 30:
            self.flush()

    def flush(self):

        if len(self.save_cache) == 0:
            return

        bulk = self.db_core.initialize_unordered_bulk_op()
        for i, r in self.save_cache:
            bulk.find({'_id': i}).upsert().replace_one(r)
        res = bulk.execute()
        lg.debug("Modified %d records", res['nModified'])
        self.save_cache = []

    def load(self, madfile, sha1sum):
        """
        Load the file from the databse,
        possibly with an alternative sha1sum
        """

        if sha1sum is not None:
            lg.warning("load from record with alternative cs %s", sha1sum)
            sha1 = sha1sum
        else:

            sha1 = madfile.all['sha1sum']

        if not sha1:
            return

        mongo_id = sha1[:24]
        lg.debug("getting mad data for {}".format(
                 madfile['inputfile']))
        lg.debug(" - sha1: {}".format(sha1))
        lg.debug(" - mongo_id: {}".format(mongo_id))
        data = self.db_core.find_one({'_id': mongo_id})
#        print('preload', madfile.mad)
#        print('data', data)
        madfile.mad.update(data)
#        print('postload', madfile.mad)

    def finish(self):
        lg.debug("cleaning up")
        self.flush()
