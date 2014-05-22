import logging
import os

import fantail

from mad2.exception import MadPermissionDenied

lg = logging.getLogger(__name__)
#lg.setLevel(logging.DEBUG)


class SidecarStore():

    def __init__(self, conf):
        self.conf = conf

    def prepare(self, madfile):

        inputfile = madfile['inputfile']
        filename = madfile['filename']
        dirname = madfile['dirname']

        if os.path.isdir(inputfile):

            maddir = os.path.join(os.path.abspath(inputfile),
                                  '.mad', 'config')
            if not os.path.exists(maddir):
                os.makedirs(maddir)
            lg.debug("'{}' is a dir".format(inputfile))
            madname = os.path.join(maddir, '_root.config')

        else:
            # looking at file
            if filename[-4:] == '.mad':

                if filename[0] == '.':
                    filename = filename[1:-4]

                madname = inputfile
                inputfile = os.path.join(dirname, filename)
            else:
                inputfile = inputfile
                madname = os.path.join(dirname, '.' + filename + '.mad')

        lg.debug("madname: {}".format(madname))
        lg.debug("inputfile: {}".format(inputfile))

        if os.path.exists(madname) and not os.access(madname, os.R_OK):
            raise MadPermissionDenied()

        madfile.all['madname'] = madname
        madfile.all['filename'] = filename

        if madfile.get('orphan', False) and os.path.exists(madname):
            lg.warning("Orphaned mad file: {}".format(madname))
            lg.debug("  | can't find: {}".format(inputfile))

    def save(self, madfile):

        try:
            lg.debug("saving to %s" % madfile['madname'])
            # note the mad file data is in stack[1] - 0 is transient
            # print(self.mad)
            fantail.yaml_file_save(madfile.mad, madfile['madname'])
        except IOError, e:
            if e.errno == 36:
                lg.error("Can't save - filename too long: {}"
                         .format(self.fullpath))
            else:
                raise

    def load(self, madfile):
        lg.debug("sidecar load: %s", madfile)
        if os.path.exists(madfile['madname']):
            lg.debug("loading madfile {0}".format(madfile['madname']))

            # note the mad file data is in stack[1] - 0 is transient
            madfile.mad.update(
                fantail.yaml_file_loader(madfile['madname']))
