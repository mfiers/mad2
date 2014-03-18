import logging
import os
import re

import jinja2

import fantail

from mad2.exception import MadPermissionDenied

lg = logging.getLogger(__name__)
#lg.setLevel(logging.DEBUG)


def dummy_hook_method(*args, **kw):
    return None


class MadFile(object):
    """

    """

    def __init__(self,
                 inputfile,
                 base=fantail.Fantail(),
                 hook_method=dummy_hook_method):

        super(MadFile, self).__init__()

        self.dirmode = False

        dirname = os.path.dirname(inputfile)
        filename = os.path.basename(inputfile)

        self.mad = fantail.Fantail()
        self.all = base.copy()

        lg.debug(
            "Instantiating a madfile for '{}' / '{}'".format(
                dirname, filename))

        if os.path.isdir(inputfile):
            self.dirmode = True
            maddir = os.path.join(os.path.abspath(inputfile),
                                  '.mad', 'config')
            if not os.path.exists(maddir):
                os.makedirs(maddir)
            lg.debug("'{}' is a dir".format(inputfile))
            madname = os.path.join(maddir, '_root.config')

        else:
            # looking at a inputfile
            if filename[-4:] == '.mad':

                if filename[0] == '.':
                    filename = filename[1:-4]
                else:
                    # old style - prob needs to go
                    filename = filename[:-4]

                madname = inputfile
                inputfile = os.path.join(dirname, filename)
            else:
                inputfile = inputfile
                madname = os.path.join(dirname, '.' + filename + '.mad')

        lg.debug("madname: {}".format(madname))
        lg.debug("inputfile: {}".format(inputfile))

        if os.path.exists(madname) and not os.access(madname, os.R_OK):
            raise MadPermissionDenied()


        self.all['inputfile'] = inputfile
        self.all['dirname'] = os.path.abspath(dirname)
        self.all['filename'] = filename
        self.all['madname'] = madname

        if not os.path.exists(inputfile):
            self.all['orphan'] = True

        if self.get('orphan', False) and os.path.exists(madname):
            lg.warning("Orphaned mad file: {}".format(madname))
            lg.warning("  | can't find: {}".format(inputfile))

        self.hook_method = hook_method
        self.load()

    # Pretend to be dict
    def __getitem__(self, key):
        if key in self.mad:
            return self.mad[key]
        return self.all[key]

    def __setitem__(self, key, value):
        self.mad.__setitem__(key, value)

    def get(self, key, default=None):
        if key in self.mad:
            return self.mad[key]
        else:
            return self.all.get(key, default)

    def has_key(self, key):
        if key in self.mad:
            return True
        return key in self.all

    def keys(self):
        k = set()
        k.update(set(self.mad.keys()))
        k.update(set(self.all.keys()))
        return iter(list(k))

    def __contains__(self, key):
        if key in self.mad:
            return True
        return key in self.all

    # @property
    # def mad(self):
    #     """
    #     Return the yaco object from the stack representing
    #     the data in the madfile
    #     """
    #     return self.stack[1]

    def __str__(self):
        return '<mad2.madfile.MadFile {}>'.format(self['inputfile'])

    def get_jinja_env(self):

        def regex_sub(s, find, replace):
            """A non-optimal implementation of a regex filter"""
            return re.sub(find, replace, s)

        jenv = jinja2.Environment(
            undefined=jinja2.DebugUndefined)
        jenv.filters['re_sub'] = regex_sub
        return jenv

    def render(self, text, data):

        jenv = self.get_jinja_env()
        rendered = text
        iteration = 0

        if not isinstance(data, list):
            data = [data]
        # stack all data - to prevent potential problems
        # TODO: needs more investigation

        data_stacked = {}
        for d in data[::-1]:
            data_stacked.update(d)

        # data_staqcked.update(self)
        while '{{' in rendered or '{%' in rendered:

            if iteration > 0 and rendered == last:
                # no improvement

                break
            last = rendered
            lll = rendered

            try:
                template = jenv.from_string(rendered)
            except:
                print("problem creating template with:")
                print(rendered)
                raise

            try:
                rendered = template.render(c=data_stacked, **data_stacked)
            except jinja2.exceptions.UndefinedError, e:
                pass
            except:
                print("cannot render")
                print(rendered)
                raise

            iteration += 1

        return rendered

    def load(self):

        self.hook_method('madfile_pre_load', self)
        if os.path.exists(self['madname']):
            lg.debug("loading madfile {0}".format(self['madname']))

            #note the mad file data is in stack[1] - 0 is transient
            self.mad.update(fantail.yaml_file_loader(self['madname']))

        self.hook_method('madfile_load', self)
        self.hook_method('madfile_post_load', self)


    def save(self):
        self.hook_method('madfile_save', self)
        try:
            lg.debug("saving to %s" % self['madname'])
            #note the mad file data is in stack[1] - 0 is transient
            #print(self.mad)
            fantail.yaml_file_save(self.mad, self['madname'])
        except IOError, e:
            if e.errno == 63:
                lg.warning("Can't save - inputfile too long: {}"
                           .format(self.fullpath))
            else:
                raise
        self.hook_method('madfile_post_save', self)

    def pretty(self):
        import pprint
        return pprint.pformat(dict(self.all).update(self.mad))


