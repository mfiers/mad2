import os
import Yaco
import hashlib
import logging
from functools import partial

lg = logging.getLogger(__name__)

def dummy_hook_method(*args, **kw):
    return None

class MadFile(object):
    """
    
    """
    def __init__(self, filename, hash_func=None):

        if filename[-5:] == '.mad2':
            filename = filename[:-5]

        self.filename = filename
        self.madname = self.filename + '.mad2'
        self.mad = Yaco.Yaco()
        self.hook_method = dummy_hook_method
        
        #set a default hash func - unless something is specified
        def sha1(filename):
            with open(self.filename, 'rb') as F:
                sha = hashlib.sha1()
                for buf in iter(lambda: F.read(4096), b''):
                    sha.update(buf)
            return sha.hexdigest()
        if hash_func is None:
            self.hash_func = sha1
        else:
            self.hash_func = hash_func

        self.load()

    def load(self):
        
        if os.path.exists(self.madname):
            lg.debug("loading madfile {}".format(self.madname))
            self.mad.load(self.madname)
        self.hook_method('madfile_load')
        
    def save(self):
        self.hook_method('madfile_save')
        self.mad.save(self.madname)

    def checksum(self):
        checksum = self.hash_func(self.filename)
        self.mad.checksum = checksum
        lg.debug("calculated checksum: %s" % checksum)

    def pretty(self):
        return self.mad.pretty()
