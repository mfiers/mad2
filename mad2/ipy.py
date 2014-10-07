
from collections import defaultdict

import leip

from mad2 import util

from IPython.core.magic import magics_class, line_magic, cell_magic, Magics


@magics_class
class MadMadMad(Magics):

    def __init__(self, *args, **kwargs):
        super(MadMadMad, self).__init__(*args, **kwargs)

        self.madapp = leip.app('mad2')
        
        self.input = set()
        self.output = set()
        self.db = set()


    @line_magic
    def mad_provenance_input(self, line):
        "my line magic"
        files = line.strip().split()
        for f in line.strip().split():
            self.input.add(f)

    @line_magic
    def mad_provenance_output(self, line):
        "my line magic"
        files = line.strip().split()
        for f in line.strip().split():
            self.output.add(f)

    @line_magic
    def mad_provenance_db(self, line):
        "my line magic"
        files = line.strip().split()
        for f in line.strip().split():
            self.db.add(f)
        
    @line_magic
    def mad_provenance_register(self, line):
        mf = defaultdict(list)
        
        for fn in self.input:
            mf['input'].append(util.get_mad_file(self.madapp, fn))
        for fn in self.output:
            mf['output'].append(util.get_mad_file(self.madapp, fn))
        for fn in self.db:
            mf['db'].append(util.get_mad_file(self.madapp, fn))

        rv = []
        import os
        rv.append(os.getcwd())
        ip = get_ipython()
        rv.append(dir(ip))
        rv.append(ip)
        for cat in mf:
            for madfile in mf[cat]:
                madfile.load()

#                rv.append(cat)
#                rv.append(madfile)
#                rv.append(madfile['orphan'])
#                rv.append(madfile['fullpath'])
                #1madfile.save()
            
        return map(str, rv)
                      
def load_ipython_extension(ip):
    ip.register_magics(MadMadMad)
            
