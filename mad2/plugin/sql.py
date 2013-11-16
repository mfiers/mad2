from __future__ import print_function


import logging
import leip
import os
import re

lg = logging.getLogger(__name__)
#lg.setLevel(logging.DEBUG)

from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()

class SqlMadFile(Base):
    __tablename__ = 'madfile'

    id = Column(Integer, primary_key=True)

    basename = Column(String)
    dirname = Column(String)
    filesize = Column(Integer)
    host = Column(String)
    madname = Column(String)
    owner = Column(String)
    project = Column(String)
    userid = Column(String)
    username = Column(String)

    def __init__(self, maf):
        self.basename = maf.basename
        self.dirname = maf.dirname
        self.filesize = maf.all.filesize
        self.host = maf.host
        self.madname = maf.madname
        self.owner = maf.owner
        self.project = maf.project
        self.userid = maf.userid
        self.username = maf.username

    def __repr__(self):
       return "<SqlMadFile('%s')>" % (self.basename)

def get_session(app):
    global Base
    enginestr = app.conf.plugin.sql.engine
    lg.warning("opening db @ {}".format(enginestr))
    engine = create_engine(enginestr)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return Session()

def save_madfile(app, madfile):
    session = get_session(app)
    M = SqlMadFile(madfile)
    session.add(M)
    session.commit()

@leip.hook("madfile_save", 200)
def sqlsave(app, madfile):

    lg.critical("start save to %s" % app.conf.plugin.sql.engine)
    save_madfile(app, madfile)
