import errno
import logging
import os
import re

import leip

from mad2 import ui

lg = logging.getLogger(__name__)

def isint(n):
    try:
        int(n)
        return True
    except ValueError:
        return False

def dehumanize(n):
    n = n.strip().upper()
    ff = re.match("([0-9]+)([KMGT]?)", n)
    if not ff:
        lg.warning("Invalid size specification: %s", n)
        exit(-1)
    base, ext = ff.groups()
    if not ext:
        return int(base)
    mult = 1024 ** (1 + 'KMGT'.index(ext))
    return int(float(base) * mult)


@leip.arg('-I', '--ignore_mad_ignore', action='store_true',
          help='ignore mad.ignore files')
@leip.arg('-s', '--minsize', help='minimum size (may use 1k, 1M, 1G)' +
          ' default: 10k', default='10k')
@leip.arg('-n', '--no_recurse', action='store_true',
          help='no recursive search')
@leip.arg('-p', '--progress', action='store_true',
          help='show some progress indicator')
@leip.arg('-d', '--do_dot_dirs', action='store_true',
          help='do not ignore .* directories')
@leip.arg('-a', '--do_dot_files', action='store_true',
          help='do not ignore .* files')
@leip.command
def scan(app, args):
    """
    (recursively) find and print files may have a mad annotation
    """
    minsize = dehumanize(args.minsize)

    files_to_ignore = app.conf.get('plugin.find.ignore', [])

    def check_write_permission(d):
        sha1file = os.path.join(d, 'SHA1SUMS')
        # we'll assume that if we have access to the sha1 file we also
        # have access to the meta file
        if os.path.exists(sha1file):
            if os.access(sha1file, os.W_OK):
                return True
            else:
                lg.debug("No write permission on %s", sha1file)
                return False

        #no sha1file - then check for write access on the directory
        return os.access(d, os.W_OK)

    app.trans['progress.find'] = 0
    counter = 0
    #lg.setLevel(logging.DEBUG)
    for dirpath, dirnames, filenames in os.walk('.'):

        #ui.message("considering %s (%d dirs, %d files)",
        #           os.path.basename(dirpath), len(dirnames),
        #           len(filenames))

        # if it's unlikely that we're able to write sha1sums to a
        # local file, we're not going to process this file
        if not check_write_permission(dirpath):
            continue

        dirs_to_remove = set()

        #never traverse into a .mad directorie
        while '.mad' in dirnames:
            dirnames.remove('.mad')

        #remove all .* directories
        for d in dirnames:
            if (not args.do_dot_dirs) and d[0] == '.':
                dirs_to_remove.add(d)
            if not args.ignore_mad_ignore:
                madignore = os.path.join(
                    dirpath, d, 'mad.ignore')
                if os.path.exists(madignore):
                    dirs_to_remove.add(d)

        for d in dirs_to_remove:
            dirnames.remove(d)

        lg.debug(" - have %d subdirs left", len(dirnames))
        lg.debug(" - processing %d files", len(filenames))

        for f in filenames:

            ffn = os.path.join(dirpath, f)
            #lg.debug("considering file: %s", f)

            if f in files_to_ignore:
                continue

            if (not args.do_dot_files) and \
                    f[0] == '.':
                continue

            try:
                filestat = os.stat(ffn)
            except OSError as e:
                lg.debug(" --- cannot stat: %s (%d)", f, e.errno)
                if e.errno == errno.ENOENT:
                    #path does not exists - or is a broken symlink
                    continue
                else:
                    lg.warning("error scanning %s", ffn)
                    lg.warning("%s", e)
                    continue

            if minsize > 0:
                if filestat.st_size < minsize:
                    #lg.debug(" --- too small: %s (%d)", f, filestat.st_size)
                    continue

            if not os.access(ffn, os.R_OK):
                #no read permission
                continue

            app.trans['progress.find'] += 1
            counter += 1

            pp = app.trans['progress.find']
            if args.progress and  pp > 0 and pp % 2500 == 0:
                lg.warning("mad scan: found {} files".format(pp))
                lg.warning("  now in: {}".format(dirpath))

            print(ffn)

        if dirpath == '.' and args.no_recurse:
            break
