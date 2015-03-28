


import logging
import leip
import os
import re

import Yaco

lg = logging.getLogger(__name__)
#lg.setLevel(logging.DEBUG)

from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()
SESSION = None

from mad2.util import get_all_mad_files

class SqlMadInfo(Base):
    __tablename__ = 'madinfo'
    id = Column(Integer, primary_key=True)
    madfile = Column(Integer, index=True)
    key = Column(String, index=True)
    val = Column(String, index=True)

    def __init__(self, madfileId, key, value):
        self.madfile = madfileId
        self.key = key
        self.val = str(value)

class SqlMadFile(Base):
    __tablename__ = 'madfile'

    id = Column(Integer, primary_key=True)
    sha1 = Column(String, index=True)
    basename = Column(String, index=True)
    dirname = Column(String, index=True)
    host = Column(String, index=True)

    def __init__(self, maf):
        if 'sha_1' in maf.hash:
            self.sha1 = maf.hash.sha1
        else:
            self.sha1 = ""
        self.basename = maf.basename
        self.dirname = maf.dirname
        self.host = maf.host

    def __repr__(self):
       return "<SqlMadFile('%s|%s')>" % (self.basename, self.id)

def get_session(app):
    global Base, SESSION
    if SESSION != None:
        return SESSION

    enginestr = app.conf.plugin.sql.engine
    lg.debug("opening db @ {}".format(enginestr))
    engine = create_engine(enginestr)
    SESSION = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)
    return SESSION

def save_madfile(app, maf):
    lg.debug("saving {}".format(maf.basename))
    session = get_session(app)
    allrecs = session.query(SqlMadFile)\
            .filter(SqlMadFile.basename==maf.basename)\
            .filter(SqlMadFile.dirname==maf.dirname)\
            .filter(SqlMadFile.host==maf.host).all()

    if len(allrecs) > 1:
        lg.error("duplicates in database :(")
        m = allrecs[0]
    elif len(allrecs) == 0:
        m = SqlMadFile(maf)
        session.add(m)
        session.commit()
    else:
        m = allrecs[0]

    #remove all old key/value pairs
    madid = m.id

    session.query(SqlMadInfo)\
            .filter(SqlMadInfo.madfile == madid)\
            .delete()
    session.commit()

    for ky in list(maf.keys()):
        vl = maf.get(ky, None)
        if vl is None:
            continue
        if isinstance(vl, Yaco.Yaco):
            continue

        m = SqlMadInfo(madid, ky, vl)
        session.add(m)
        session.commit()

    return

@leip.arg('value')
@leip.arg('key')
@leip.command
def dbfind(app, args):
    """
    find using the database
    """
    session = get_session(app)
    allrecs = session.query(SqlMadInfo)\
            .filter(SqlMadInfo.key==args.key)\
            .filter(SqlMadInfo.val==args.value).all()
    ids = [x.madfile for x in allrecs]
    mafs = [session.query(SqlMadFile).get(x) for x in ids]
    for m in mafs:
        print(m.host + ':' + os.path.join(m.dirname, m.basename))

@leip.arg('file', nargs='*')
@leip.command
def dbsave(app, args):
    """
    print a single value from a single file
    """
    for madfile in get_all_mad_files(app, args):
        save_madfile(app, madfile)
        print(madfile.filename)

@leip.hook("madfile_save", 200)
def sql_hook_save(app, madfile):
    lg.debug("start save to %s" % app.conf.plugin.sql.engine)
    save_madfile(app, madfile)
