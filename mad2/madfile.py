import os
import Yaco
#import hashlib
import logging
from functools import partial

lg = logging.getLogger(__name__)

def dummy_hook_method(*args, **kw):
    return None

class MadFile(object):
    """
    
    """
    #def __init__(self, filename, hash_func=None):
    def __init__(self, filename):

        if filename[-4:] == '.mad':
            filename = filename[:-4]

        self.filename = filename
        self.madname = self.filename + '.mad'
        self.mad = Yaco.Yaco()
        self.hook_method = dummy_hook_method
        
        self.load()

    def render(self, text, base):

        if isinstance(base, Yaco.PolyYaco):
            data= base.merge()
        else:
            data = copy.copy(base)

        data.update(self.mad)
        data['madname'] = self.madname
        data['filename'] = self.filename

        rendered = text
        last = rendered
        iteration = 0
        while '{{' in rendered or '{%' in rendered:
            if iteration > 0 and rendered == last:
                #no improvement
                break
        template = Template(text)   
        rendered = template.render(data)

        
    #     if dry:
    #         lg.warning("Executing: {}".format(rendered))
    #     else:
    #         lg.warning("Executing: {}".format(rendered))
    #         os.system(rendered)
    # def execute(self, cl, dry=False):
    #     """
    #     execute a command line in the context of this object

    #     :param dry: do a dry run
    #     :type dry: boolean
    #     """
    #     from jinja2 import Template
    #     import copy

    #     data = copy.copy(self.mad.get_data())
    #     data['madname'] = self.madname
    #     data['filename'] = self.filename

    #     template = Template(cl)
    #     rendered = template.render(data)
    #     if dry:
    #         lg.warning("Executing: {}".format(rendered))
    #     else:
    #         lg.warning("Executing: {}".format(rendered))
    #         os.system(rendered)
        
        
    # def defer(self, cl):
    #     lg.debug("deferring for later execution: {0}".format(cl))
    #     if not 'mad_defer' in  self.mad:
    #         self.mad.mad_defer = []
    #     if not cl in self.mad.mad_defer:
    #         self.mad.mad_defer.append(cl)
            
    # def catchup(self):
    #     """
    #     execute all commands!

    #     Execute all deferred commands - first step is to extract the
    #     list of commands that need to run and save the madfile without
    #     that list. This allows the commands to change the madfile
    #     without this operation re-overwriting the madfile
    #     """

    #     cls = self.mad.mad_defer
    #     self.mad.mad_defer = []
    #     self.save()
    #     for cl in cls:
    #         self.execute(cl)

    def load(self):        
        if os.path.exists(self.madname):
            lg.debug("loading madfile {}".format(self.madname))
            self.mad.load(self.madname)
        self.hook_method('madfile_load')
        
    def save(self):
        self.hook_method('madfile_save')
        self.mad.save(self.madname)

    # def checksum(self):
    #     checksum = self.hash_func(self.filename)
    #     self.mad.checksum = checksum
    #     lg.debug("calculated checksum: %s" % checksum)

    def pretty(self):
        return self.mad.pretty()
