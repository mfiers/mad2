

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
            if not uri[-7:] == 'db/data':
                lg.critical("Invalid neo4j uri %s", uri)
                lg.critical(" | should end with /db/data")
                exit(-1)
            lg.debug("Neo4j db: %s", uri)
            self.db = neo4j.GraphDatabaseService(uri)
        else:
            lg.debug("Neo4j db: default")
            self.db = neo4j.GraphDatabaseService()

        self.sha1_index = self.db.get_or_create_index(
                neo4j.Node, "Sha1")
        self.file_index = self.db.get_or_create_index(
                neo4j.Node, "File")
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
    uri = madfile.uri
    user = madfile.username
    host = madfile.host
    sha1 = madfile.hash.sha1
    project = madfile.project

    #del simple['username']
    lg.debug("Saving %s to neo4j", madfile.basename)
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
    if not app.conf.plugin.neo4j.get('autostore', False):
        return
    lg.debug("start neo4j autosave")
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
    print(query_txt)

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


def _dup_report_sum(fileset):
    filesize = fileset[0].filesize
    if not filesize:
        return [], 0
    used_space = 0
    for f in fileset:
        nlink = 1
        if f.nlink: nlink = f.nlink
        used_space += filesize / float(nlink)
    if used_space > filesize:
        to_file = []
        for i, f in enumerate(fileset):
            to_file.append(
                "\t".join([str(f[c]) for c in f.columns])
                + "\n")
        return to_file, used_space - filesize
    else:
        return [], 0

@leip.arg('report_file', help="Report file", nargs='?')
@leip.command
def neo_dup(app, args):
    """
    Summarize duplicate files
    """
    query_txt = app.conf.plugin.neo4j.cypher.duplicates
    lg.debug(query_txt)
    session = cypher.Session(app.conf.plugin.neo4j.uri)
    tx = session.create_transaction()
    tx.append(query_txt)
    res = tx.commit()

    this_sha = None
    fileset = []
    wasted_space = 0
    norec = 0
    repfile = []

    for i, r in enumerate(res[0]):
        norec += 1
        if r.sha1 != this_sha:
            if not this_sha is None:
                tf, ws = _dup_report_sum(fileset)
                wasted_space += ws
                repfile.extend(tf)
            this_sha = r.sha1
            fileset = []
        fileset.append(r)

    if fileset and not this_sha is None:
        tf, ws = _dup_report_sum(fileset)
        wasted_space += ws
        repfile.extend(tf)

    lg.info("retrieved %d records", norec)

    print("Wasted space: {:.0f} Mb".format(wasted_space))

    print(args.report_file)
    if args.report_file:
        with open(args.report_file[0], 'w') as F:
            for line in repfile:
                F.write(line)

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

