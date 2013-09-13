import os
import Yaco
import copy
import logging

from jinja2 import Template

lg = logging.getLogger(__name__)


def dummy_hook_method(*args, **kw):
    return None


class MadFile(object):
    """

    """
    #def __init__(self, filename, hash_func=None):
    def __init__(self, filename, hook_method=dummy_hook_method):

        if filename[-4:] == '.mad':
            if filename[0] == '.':
                filename = filename[1:-4]


        self.mad = Yaco.Yaco()
        self.otf = Yaco.Yaco()  # on the fly calculated information

        self.otf.filename = filename
        self.otf.madname = '.' + self.otf.filename + '.mad'

        self.hook_method = hook_method
        self.load()

    def __getitem__(self, item):
        if item in self.mad:
            return self.mad[item]
        elif item in self.otf:
            return self.otf[item]
        raise KeyError()

    __getattr__ = __getitem__

    def data(self, on_top_of={}):
        """Render data into a dict like format
        """
        if isinstance(on_top_of, Yaco.Yaco) or \
            isinstance(on_top_of, Yaco.PolyYaco):
            data = on_top_of.simple()

        data.update(self.otf.simple())
        data.update(self.mad.simple())
        return data

    def render(self, text, base):

        if isinstance(base, Yaco.PolyYaco):
            data = base.merge()
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
            template = Template(rendered)
            rendered = template.render(data)
        return rendered

    def load(self):
        if os.path.exists(self.madname):
            lg.debug("loading madfile {0}".format(self.madname))
            self.mad.load(self.madname)
            self.hook_method('madfile_load', self)

    def save(self):
        self.hook_method('madfile_save', self)
        self.mad.save(self.madname)

    def pretty(self):
        return self.mad.pretty()
