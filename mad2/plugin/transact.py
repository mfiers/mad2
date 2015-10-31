
from datetime import datetime
import functools
from hashlib import sha1
import itertools
import logging
import os
import random
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


@leip.subparser
def ta(app, args):
    """
    Transaction management
    """
    pass

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


@leip.arg('transaction_id')
@leip.subcommand(ta, "show")
def ta_show(app, args):
    """Show transaction"""
    db_t, db_s2t = get_mongo_transact_db(app)
    rec = db_t.find_one({"_id": args.transaction_id})
    print(rec)
    ids2hash = [rec['salt'], rec['uname'], rec['time'],
                rec['host'], rec['cl']]
    for fo in rec['io']:
        ids2hash.append(fo['sha1sum'])
    tcheck = sha1()
    for i, _ in enumerate(sorted(ids2hash)):
        #print(i, _)
        tcheck.update(_.encode('UTF-8'))

    tcheck = tcheck.hexdigest()
    if not tcheck == rec['_id']:
        lg.warning("transaction checksum mismatch")
    print(yaml.safe_dump(rec, default_flow_style=False))


@functools.lru_cache(128)
def exec_expander(exefile):
    """
    determine full path executable
    """
    if os.path.exists(exefile):
        return newfile
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

    items_to_hash.append(time.isoformat())
    transact['time'] = time

    if not args.script is None and os.path.exists(args.script):
        with open(args.script) as F:
            cl = F.read()
        transact['cl'] = cl
        items_to_hash.append(cl)

    db_t, db_s2t = get_mongo_transact_db(app)

    all_file_shasums = []

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
            madfile.save()
            items_to_hash.append(madfile.mad['sha1sum'])
            all_file_shasums.append(madfile.mad['sha1sum'])

            transact['io'].append(
                dict(filename=filename,
                     category=cat,
                     group=group,
                     sha1sum=madfile.mad['sha1sum']))

    thash = sha1()
    items_to_hash.sort()
    for i, _ in enumerate(items_to_hash):
        #print(i, _)
        thash.update(_.encode('UTF-8'))

    thash = thash.hexdigest()
    lg.debug("transaction hash: %s", thash)
    transact['_id'] = thash

    # store transaction
    insert_id = db_t.insert_one(transact)
    # store sha1sum to transaction links
    db_s2t.insert_many([dict(transaction_id=thash, sha1sum=x)
                        for x in set(all_file_shasums)])
