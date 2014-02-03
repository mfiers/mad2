from __future__ import print_function

import logging
import os
from pprint import pprint
import re

import leip
import Yaco

from py2neo import neo4j
from py2neo import cypher

neo_logger = logging.getLogger(
    "py2neo.packages.httpstream.http")
neo_logger.setLevel(logging.WARNING)

from mad2.util import get_all_mad_files

lg = logging.getLogger(__name__)
#lg.setLevel(logging.INFO)

class GDB:
    #Dummy object that holds data on a database
    def __init__(self, app):
        self.app = app #leip application - access to conf
        if 'uri' in self.app.conf.plugin.neo4j:
            uri = self.app.conf.plugin.neo4j.uri
            self.db = neo4j.GraphDatabaseService(uri)
        else:
            self.db = neo4j.GraphDatabaseService()

        self.file_index = self.db.get_or_create_index(
                neo4j.Node, "File")
        self.sha1_index = self.db.get_or_create_index(
                neo4j.Node, "Sha1")
        self.host_index = self.db.get_or_create_index(
                neo4j.Node, "Host")
        self.user_index = self.db.get_or_create_index(
                neo4j.Node, "User")
        self.project_index = self.db.get_or_create_index(
                neo4j.Node, "Project")

DB = None
def get_db(app):
    global DB
    if DB is None:
        DB = GDB(app)
    return DB

def neo_save_madfile(app, madfile):
    db = get_db(app)
    simple = madfile.simple()
    uri = madfile.all.uri
    user = madfile.all.username
    host = madfile.all.host
    sha1 = madfile.mad.hash.sha1
    project = madfile.all.project

    del simple['username']

    if project:
        del simple['project']

    file_node = db.file_index.get_or_create("uri", uri, simple)
    file_node.delete_properties()

    file_node.set_properties(simple)

    file_node.add_labels('file')

    if project:
        proj_node = db.project_index.get_or_create(
            "project", project, {'project' : project} )
        proj_node.add_labels('project')
        proj_node.get_or_create_path("HAS_FILE", file_node)

    if sha1:
        hash_node = db.sha1_index.get_or_create(
            "sha1", sha1, {'sha1' : sha1} )
        hash_node.add_labels('sha1')
        file_node.get_or_create_path("HAS_SHA1", hash_node)

    host_node = db.host_index.get_or_create("host", host,
        {"host" : host})
    host_node.add_labels('host')
    host_node.get_or_create_path("HAS_FILE", file_node)

    user_node = db.host_index.get_or_create("user", user,
        {"user" : user})
    user_node.add_labels('user')
    user_node.get_or_create_path("OWNS_FILE", file_node)


@leip.command
def neo_clean(app, args):
    db = get_db(app)
    db.db.clear()

@leip.hook("madfile_save", 200)
def neo_hook_save(app, madfile):
    lg.debug("start neo4j save")
    neo_save_madfile(app, madfile)


@leip.arg('query', help='cypher query to run')
@leip.command
def neo_query(app, args):
    """
    print a single value from a single file
    """
    lg.warning("running query: %s", args.query)

    queries = app.conf.plugin.neo4j.cypher
    query_txt = queries.get(args.query)

    session = cypher.Session(app.conf.plugin.neo4j.uri)
    tx = session.create_transaction()
    tx.append(query_txt)
    res = tx.commit()
    cols = []
    for i, r in enumerate(res[0]):
        if i == 0:
            cols = r.columns
            print("#" + "\t".join(cols))
        print("\t".join([str(r[c]) for c in cols]))


def _dup_report_sum(handle, fileset):
    filesize = fileset[0].filesize
    used_space = 0
    for f in fileset:
        nlink = 1
        if f.nlink: nlink = f.nlink
        used_space += filesize / float(nlink)
    if used_space > filesize:
        for i, f in enumerate(fileset):
            handle.write(
                "\t".join([str(f[c]) for c in f.columns])
                + "\n")
        return used_space - filesize
    else:
        return 0

@leip.arg('report_file', help="Report file")
@leip.command
def neo_dup(app, args):
    """
    Summarize duplicate files
    """
    query_txt = app.conf.plugin.neo4j.cypher.duplicates
    print(query_txt)
    session = cypher.Session(app.conf.plugin.neo4j.uri)
    tx = session.create_transaction()
    tx.append(query_txt)
    res = tx.commit()

    this_sha = None
    fileset = []
    wasted_space = 0
    norec = 0
    with open(args.report_file, 'w') as F:
        for i, r in enumerate(res[0]):
            norec += 1
            if r.sha1 != this_sha:
                if not this_sha is None:
                    wasted_space += _dup_report_sum(F, fileset)
                this_sha = r.sha1
                fileset = []
            fileset.append(r)

        if not this_sha is None:
            wasted_space += _dup_report_sum(F, fileset)
    lg.info("retrieved %d records", norec)

    ws_g = wasted_space / (1024**3)
    print("Wasted space: {:.0f} ({:.2f}Gb)".format(wasted_space, ws_g))


@leip.arg('file', nargs='*')
@leip.command
def neo_save(app, args):
    """
    print a single value from a single file
    """
    for madfile in get_all_mad_files(app, args):
        if madfile.orphan is True:
            pass # not doing orphans
        else:
            neo_save_madfile(app, madfile)
            print(madfile.filename)

