from collections import defaultdict
from datetime import datetime
import functools
from hashlib import sha1
import itertools
import logging
import os

import shlex
import socket
import subprocess as sp
from uuid import uuid4

from bson.objectid import ObjectId
import sh
import humanize
from termcolor import cprint
import yaml


lg = logging.getLogger(__name__)

import leip
from mad2.util import get_mad_file, get_mongo_transact_db
from mad2.util import get_mongo_transient_db, get_mongo_core_db

@leip.subparser
def ta(app, args):
    """
    Transaction management
    """
    pass


@leip.arg('object')
@leip.subcommand(ta, "tree")
def ta_tree(app, args):

    import networkx as nx

    G = nx.DiGraph()

    db_t, db_s2t = get_mongo_transact_db(app)
    trans_db = get_mongo_transient_db(app)


    if len(args.object) == 40 and not os.path.exists(args.object):
        sha1sum = args.object
    else:
        madfile = get_mad_file(app, args.object)
        sha1sum = madfile['sha1sum']

    def _get_trarec(sha1sum):
        rv = defaultdict(set)
        for rec in trans_db.find(dict(sha1sum=sha1sum)):
            for field in ['project', 'filename', 'filesize', 'analyst',
                          'pi', 'username', 'fullpath']:
                if field in rec:
                    rv[field].add(rec[field])
        return {k: ';'.join(map(str, v)) for (k, v) in rv.items()}

    def _add_node(G, sha1sum):
        if sha1sum in G:
            return

        G.add_node(sha1sum)
        sdata = _get_trarec(sha1sum)
        G.node[sha1sum].update(sdata)

    _add_node(G, sha1sum)

    sha1sum_processed = set()

    def _find_relations_shasum(G, sha1sum):
        if sha1sum in sha1sum_processed:
            return

        sha1sum_processed.add(sha1sum)

        for s2t in db_s2t.find(dict(sha1sum=sha1sum)):
            tra = db_t.find_one(dict(_id=s2t['transaction_id']))
            io = tra['io']
            ioo = [x for x in io if x['category'] == 'output']
            if len(ioo) == 0:
                continue

            for fa, fb in itertools.product(io, ioo):
                if fa == fb:
                    continue
                fas, fbs = fa['sha1sum'], fb['sha1sum']
                _add_node(G, fas)
                _add_node(G, fbs)
                ltype = fa['category']
                if ltype == 'output':
                    ltype = 'sibling'
                G.add_edge(fas, fbs)
                G[fas][fbs]['count'] = G[fas][fbs].get('count', 0) + 1
                G[fas][fbs]['type'] = ltype

                _find_relations_shasum(G, fas)
                _find_relations_shasum(G, fbs)


    _find_relations_shasum(G, sha1sum)
    nx.write_graphml(G, 'test.graphml')



@leip.arg('object', help='show info on either a sha1sum or a file')
@leip.subcommand(ta, "sha1sum")
def ta_sha1sum(app, args):
    """Show transactions associated with a sha1sum"""
    db_t, db_s2t = get_mongo_transact_db(app)

    if len(args.object) == 40 and not os.path.exists(args.object):
        sha1sum = args.object
    else:
        madfile = get_mad_file(app, args.object)
        sha1sum = madfile['sha1sum']

    for s2t in db_s2t.find(dict(sha1sum=sha1sum)):
        tra = db_t.find_one(dict(_id=s2t['transaction_id']))
        natime = humanize.naturaldate(tra['time'])
        for io in tra['io']:
            if io['sha1sum'] == sha1sum:
                ncl = " ".join(shlex.split(tra.get('cl', 'n.a.')))
                if len(ncl) > 50:
                    ncl = ncl[:47] + '...'
                cprint(tra['_id'], color='cyan', end=' (')
                cprint(io['category'], color='yellow', end=') ')
                cprint(natime, color='green', end=": ")
                cprint(ncl)


@leip.arg('object', help='file, sha1sum or transaction id')
@leip.subcommand(ta, "show")
def ta_show(app, args):
    """Show transaction"""
    db_t, db_s2t = get_mongo_transact_db(app)
    obj = args.object
    if os.path.exists(obj):
        return ta_sha1sum(app, args)

    rec = db_t.find_one({"_id": args.object})
    if rec is None:
        print("No transaction found")
        exit()

    ids2hash = [
        rec['salt'],
        rec['uname'],
        rec['time'].isoformat(),
        rec['host'],
        rec['cl']]

    for fo in rec['io']:
        ids2hash.append(fo['sha1sum'])

    tcheck = sha1()

    for i, _ in enumerate(sorted(ids2hash)):
        tcheck.update(_.encode('UTF-8'))

    tcheck = tcheck.hexdigest()
    if not tcheck == rec['_id']:
        lg.warning("transaction checksum mismatch")
    else:
        lg.info("transaction checksum match")

    print(yaml.safe_dump(rec, default_flow_style=False))


@functools.lru_cache(128)
def exec_expander(exefile):
    """
    determine full path executable
    """
    if os.path.exists(exefile):
        return exefile
    fp = sh.which(exefile).strip().split("\n")
    if len(fp) > 0:
        return fp[0]
    else:
        return exefile


@leip.arg('--input', action='append', help='add an "input" file')
@leip.arg('--output', action='append', help='add an "output" file')
@leip.arg('--db', action='append', help='add an "db" file')
@leip.arg('--executable', action='append', help='add an "executable" file')
@leip.arg('--misc', action='append', help='add an "miscellaneous" file')
@leip.arg('--script', help='file containing the executed command line')
@leip.arg('--time', help='transaction generation time')
@leip.subcommand(ta, "add")
def ta_add(app, args):
    """Record a new transaction

    All files are put in a group, by default, this group has the same
    name as the category, but when more group names are required, they
    can be specified using a colon (e.g. fq_input:filename.fq)

    """
    salt = str(uuid4())
    uname = sp.getoutput(['uname -a'])
    host = socket.gethostname()

    items_to_hash = [salt, uname, host]

    transact = dict(io=[],
                    salt=salt,
                    host=host,
                    uname=uname)
    if args.time:
        import dateutil.parser
        time = dateutil.parser.parse(args.time)
    else:
        time = datetime.utcnow()

    time = time.replace(microsecond=0)

    items_to_hash.append(time.isoformat())
    transact['time'] = time

    if args.cl:
        transact['cl'] = args.cl.strip()
        items_to_hash.append(transact['cl'])

    elif args.script is not None and os.path.exists(args.script):
        with open(args.script) as F:
            cl = F.read()
        transact['cl'] = cl
        items_to_hash.append(cl)

    db_t, db_s2t = get_mongo_transact_db(app)

    all_file_shasums = []

    to_propagate = {}
    do_not_propagate = set()

    for cat in 'input output db executable misc'.split():
        filenames = getattr(args, cat)
        if filenames is None:
            continue

        for filename in filenames:

            group = cat
            if ':' in filename:
                group, filename = filename.split(':', 1)

            if cat == 'executable':
                filename = exec_expander(filename)

            if not os.path.exists(filename):
                lg.critical("all files of transaction must exist")
                lg.critical("cannot find %s", filename)
                exit(-1)

            madfile = get_mad_file(app, filename)

            if cat == 'input':
                # find propagateable properties
                for k, v in madfile.mad.items():
                    propable = app.conf['keywords'][k].get('propagate', False)
                    if not propable:
                        continue
                    if k in do_not_propagate:
                        continue
                    elif k in to_propagate:
                        if to_propagate[k] != v:
                            # if different values for var k in input
                            # do not propagate - exclude from further
                            # consideration
                            do_not_propagate.add(k)
                            del to_propagate[k]
                    else:
                        to_propagate[k] = v

            if cat == 'output' and to_propagate:
                for k, v in to_propagate.items():
                    if k not in madfile.mad:
                        lg.warning("propagating %s='%s' for %s", k, v, filename)
                        madfile.mad[k] = v
                madfile.mad.update(to_propagate)

            madfile.save()

            items_to_hash.append(madfile.mad['sha1sum'])
            all_file_shasums.append(madfile.mad['sha1sum'])

            transact['io'].append(
                dict(filename=filename,
                     category=cat,
                     group=group,
                     sha1sum=madfile.mad['sha1sum']))

    thash = sha1()
    for i, _ in enumerate(sorted(items_to_hash)):
        thash.update(_.encode('UTF-8'))

    thash = thash.hexdigest()
    lg.debug("transaction hash: %s", thash)
    transact['_id'] = thash

    # store transaction
    db_t.insert_one(transact)

    # store sha1sum to transaction links
    db_s2t.insert_many([dict(transaction_id=thash, sha1sum=x)
                        for x in set(all_file_shasums)])
