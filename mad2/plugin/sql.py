from __future__ import print_function


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
    #filesize = Column(Integer)
    #madname = Column(String)
    #userid = Column(String)
    #userid = Column(String)
    #username = Column(String)

    def __init__(self, maf):
        if maf.hash.has_key('sha_1'):
            self.sha1 = maf.hash.sha1
        else:
            self.sha1 = ""
        self.basename = maf.basename
        self.dirname = maf.dirname
        self.host = maf.host
        
        #self.filesize = maf.all.filesize
        #self.madname = maf.madname
        #self.owner = maf.username
        #self.userid = maf.userid
        #self.username = maf.username

    def __repr__(self):
       return "<SqlMadFile('%s|%s')>" % (self.basename, self.id)

def get_session(app):
    global Base
    enginestr = app.conf.plugin.sql.engine
    lg.warning("opening db @ {}".format(enginestr))
    engine = create_engine(enginestr)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return Session()

def save_madfile(app, maf):
    lg.warning("saving {}".format(maf.basename))
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

    session.query(SqlMadInfo)\
            .filter(SqlMadInfo.madfile == m.id)\
            .delete()
    session.commit()

    for ky in maf.keys():
        vl = maf.get(ky, None)
        if vl is None: 
            continue
        if isinstance(vl, Yaco.Yaco):
            continue
        #print(m.id, ky, vl)
        m = SqlMadInfo(m.id, ky, vl)
        session.add(m)
    session.commit()

    return


@leip.hook("madfile_save", 200)
def sqlsave(app, madfile):

    lg.critical("start save to %s" % app.conf.plugin.sql.engine)
    save_madfile(app, madfile)
