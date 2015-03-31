
from datetime import datetime
import json
import logging
import os
import re
import socket

import humanize
import leip
from mad2.util import get_mongo_transient_db
from termcolor import cprint

lg = logging.getLogger(__name__)

@leip.hook("madfile_init", 2000)
def ipyscan(app, madfile):

    if not 'extension' in madfile:
        return

    if madfile['extension'] != 'ipynb':
        return

    try:
        with open(madfile['inputfile']) as F:
            data = json.load(F)
    except ValueError:
        lg.warning('Cannot json load ipynb file: %s', madfile['inputfile'])
        return

    metadata = data.get('metadata')
    
    if not metadata:
        return
            
    kernelname = metadata.get('kernelspec', {}).get('display_name')
    if kernelname is None:
        kernelname = 'unknown'

    madfile.mad['ipy_kernel'] = kernelname
    madfile.mad['ipy_nbformat'] = data.get('nbformat', 'unknown')
    
    metamad = metadata.get('mad')
    if not metamad is None:
        madfile.mad.update(metamad)
    
@leip.arg('-l', '--limit', default=5, type=int)
@leip.command
def notebooks(app, args):
    db = get_mongo_transient_db(app)

    curdir = os.path.abspath(os.getcwd())
    rex = re.compile('^' + re.escape(curdir) + '.*\.ipynb$', re.IGNORECASE)
    host = socket.gethostname()
    
    query = {
        'extension' : 'ipynb',
        'host': host,
        'fullpath': {'$regex': rex}
        }
    
    results = db.find(query).sort('mtime', -1).limit(args.limit)
    
    for res in results:
        relpath = os.path.relpath(res['fullpath'])
        atime = res['mtime']
        dtime = humanize.naturaltime(datetime.utcnow() - atime)
        host = res['host']
        project = res.get('project')
        kernel = res['ipy_kernel']
        
        if not relpath.startswith('.'):
            relpath = './' + relpath
        cprint(dtime, 'yellow', end=', ')
        cprint(kernel, 'green', end="")
        if not project is None:
            print(", ", end="")
            cprint(project, 'red')
        else:
            print()
        print("    ", relpath)
    
