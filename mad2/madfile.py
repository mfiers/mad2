import logging
import os

import fantail

from mad2.exception import MadPermissionDenied
from mad2.recrender import recrender

lg = logging.getLogger(__name__)
# lg.setLevel(logging.DEBUG)


def dummy_hook_method(*args, **kw):
    return None


STORES = None

class MadFile(fantail.Fanstack):

    """
    Represents a single file
    """

    def __init__(self,
                 inputfile,
                 stores=None,
                 base=fantail.Fantail(),
                 hook_method=dummy_hook_method):


        self.stores = stores

        lg.debug('madfile start %s', inputfile)
        super(MadFile, self).__init__(
            stack=[fantail.Fantail(),
                   base.copy()])


        self.dirmode = False

        dirname = os.path.dirname(inputfile)
        filename = os.path.basename(inputfile)

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
            lg.debug("  | can't find: {}".format(inputfile))

        self.hook_method = hook_method
        self.load()

        # if not 'uuid' in self.mad:
        #     _uuid = str(uuid.uuid4()).replace('-', '')[:24]
        #     lg.debug("initial uuid assignment: {}".format(_uuid))
        #     self.mad['uuid'] = _uuid
        #     self.save()

    def render(self, template, data):
        """
        Render a template from, adding self to the context
        """
        if not isinstance(data, list):
            data = [data]
        return recrender(template, [self] + data)

    @property
    def mad(self):
        return self.stack[0]

    @property
    def all(self):
        return self.stack[1]

    def __str__(self):
        return '<mad2.madfile.MadFile {}>'.format(self['inputfile'])


    def load(self):

        self.hook_method('madfile_pre_load', self)
        if os.path.exists(self['madname']):
            lg.debug("loading madfile {0}".format(self['madname']))

            # note the mad file data is in stack[1] - 0 is transient
            self.mad.update(fantail.yaml_file_loader(self['madname']))

        self.hook_method('madfile_load', self)
        self.hook_method('madfile_post_load', self)

    def save(self):
        self.hook_method('madfile_save', self)
        try:
            lg.debug("saving to %s" % self['madname'])
            # note the mad file data is in stack[1] - 0 is transient
            # print(self.mad)
            fantail.yaml_file_save(self.mad, self['madname'])
        except IOError, e:
            if e.errno == 36:
                lg.error("Can't save - filename too long: {}"
                           .format(self.fullpath))
            else:
                raise
        self.hook_method('madfile_post_save', self)

    def pretty(self):
        import pprint
        return pprint.pformat(dict(self.all).update(self.mad))
