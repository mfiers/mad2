from __future__ import print_function

import re
import logging
import sys

import leip
from mad2.util import  get_filenames, get_all_mad_files

lg = logging.getLogger(__name__)

@leip.arg('-s', '--sep',  help='separator', default='\t')
@leip.arg('key', help='keys to display', nargs='+')
@leip.command
def table(app, args):
    """
    Create a table 
    """
    for madfile in get_all_mad_files(app, args):
        values = []
        for k in args.key:
            if k in madfile.mad:
                values.append(madfile.mad[k])
            elif k in madfile.__dict__:
                values.append(madfile.__dict__[k])
            else:
                values.append('')
        print(args.sep.join(map(str, values)))

                
    
    


            
