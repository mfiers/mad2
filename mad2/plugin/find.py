from __future__ import print_function

import logging
import os
import sys

import leip

lg = logging.getLogger(__name__)

@leip.arg('-n', '--no_recurse', action='store_true', help='no recursive search')
@leip.arg('-a', '--all', action='store_true', help='do not ignore .* directories')

@leip.command
def find(app, args):
    """
    (recursively) find and print files already mad annotated
    """
    for dirpath, dirnames, filenames in os.walk('.'):
        for f in filenames:

            #never traverse into a .mad directorie
            while '.mad' in dirnames:
                dirnames.remove('.mad')

            if not args.all:
                #remove all .* directories
                to_remove = []
                for d in dirnames:
                    if d[0] == '.':
                        to_remove.append(d)
                for d in to_remove:
                    dirnames.remove(d)

            #find .mad files
            if f[0] == '.' and f[-4:] == '.mad':
                print(os.path.join(dirpath, f[1:-4]))
                
        if dirpath == '.' and args.no_recurse:
            break