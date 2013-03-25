from __future__ import print_function

import re
import logging
import sys

import leip
from mad2.util import  get_filenames, get_all_mad_files

lg = logging.getLogger(__name__)

@leip.arg('file', nargs='*')
@leip.command
def init(app, args):
    """
    Initialize a file, based on extension
    """

    exti = app.conf.plugin.init.ext
    print(exti)

    for madfile in get_all_mad_files(app, args):
        for ext in exti:
            extdot = '.' + ext
            if madfile.filename[-1 * len(extdot):] == extdot:
                madfile.mad.update(exti[ext])
        madfile.save()

                
    
    


            
