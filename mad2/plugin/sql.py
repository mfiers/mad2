from __future__ import print_function


import logging
import leip
import os
import re

from sqlalchemy import create_engine
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
        self.filesize = maf.filesize
        self.host = maf.host
        self.madname = maf.madname
        self.owner = maf.owner
        self.project = maf.project
        self.userid = maf.userid
        self.username = maf.username

    def __repr__(self):
       return "<File('%s','%s', '%s')>" % (self.basename)


from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)
lg.setLevel(logging.DEBUG)

def get_engine():
    enginestr = app.conf.plugin.sql.engine
    engine = create_engine(enginestr)


@leip.hook("madfile_save", 200)
def sqlsave(app, madfile):

    lg.debug("start save to %s" % app.conf.plugin.sql.engine)
