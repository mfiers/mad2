from datetime import datetime
import logging
import os

import fantail
import socket

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
                 sha1sum=None,
                 base=fantail.Fantail(),
                 hook_method=dummy_hook_method):

        self.stores = stores
        self.hook_method = hook_method

        lg.debug('madfile start %s', inputfile)
        super(MadFile, self).__init__(
            stack=[fantail.Fantail(),
                   base.copy()])

        self.dirmode = False
        if os.path.isdir(inputfile):
            self.dirmode = True
            dirname = inputfile
            filename = ''
        else:
            dirname = os.path.dirname(inputfile)
            filename = os.path.basename(inputfile)

        if os.path.islink(inputfile):
            self.all['orphan'] = not os.path.exists(inputfile)

        lg.debug(
            "Instantiating a madfile for '{}' / '{}'".format(
                dirname, filename))

        self.all['inputfile'] = inputfile
        self.all['dirname'] = os.path.abspath(dirname)
        self.all['filename'] = filename
        self.all['fullpath'] = os.path.abspath(os.path.realpath(inputfile))

        self.hook_method('madfile_init', self)

        for s in self.stores:
            store = self.stores[s]
            store.prepare(self)
        self.load()

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

#    def check_sha1sum(self):
#        import mad2.hash
#        mad2.hash.get_sha1sum_mad(self)

    def on_change(self):
        # call when the file has been changed
        pass

    def on_delete(self):
        # call when the file has been is deleted
        pass

    def flush(self):
        """
        Flush stores
        """
        self.hook_method('flush')
        for s in self.stores:
            store = self.stores[s]
            store.flush()

    def delete(self):
        """
        """
        self.hook_method('madfile_delete', self)
        for s in self.stores:
            store = self.stores[s]
            store.delete(self)

    def load(self, sha1sum=None):
        """
        load the record from the database, possibly with an
        alternative id (used when files change)
        """

        self.all['orphan'] = False \
            if os.path.exists(self.all['inputfile']) \
            else True

        if sha1sum is None:
            # if a sha1sum is specified - do not call hooks
            # just get the data from the storage
            self.hook_method('madfile_pre_load', self)

        fis = self.get('filesize', -1)
        if fis < 1:
            lg.info("file size zero, skip: {}".format(self['inputfile']))
        else:
            for s in self.stores:
                store = self.stores[s]
                store.load(self, sha1sum=sha1sum)

        if sha1sum is None:
            self.hook_method('madfile_load', self)
            self.hook_method('madfile_post_load', self)

    def save(self):
        self.hook_method('madfile_save', self)
        self.hook_method('madfile_pre_save', self)

        fis = self.get('filesize', -1)
        if fis <= 0:
            lg.info("file size zero - not annotating this")
        else:
            for s in self.stores:
                store = self.stores[s]
                store.save(self)

        self.hook_method('madfile_post_save', self)


class MadDummy(MadFile):

    def __init__(self, data_core, data_all,
                 stores=None,
                 hook_method=dummy_hook_method):

        self.stores = stores
        self.hook_method = hook_method

        super(MadFile, self).__init__(
            stack=[data_core, data_all])

        lg.debug("Instantiating a dummy madfile")

        for s in self.stores:
            store = self.stores[s]
            store.prepare(self)
