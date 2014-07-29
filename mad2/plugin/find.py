import errno
import logging
import os
import re

import leip

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
@leip.arg('-d', '--do_dot_dirs', action='store_true',
          help='do not ignore .* directories')
@leip.arg('-a', '--do_dot_files', action='store_true',
          help='do not ignore .* files')
@leip.command
def find(app, args):
    """
    (recursively) find and print files may have a mad annotation
    """
    minsize = dehumanize(args.minsize)

    #lg.setLevel(logging.DEBUG)
    for dirpath, dirnames, filenames in os.walk('.'):
        lg.debug("considering %s", dirpath)

        dirs_to_remove = set()

        if 'mad.ignore' in filenames:
            #ignore this directory - this cpature is
            #only necessary for the root
            #remove all from dirnames
            dirnames[:] = []
            continue


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

            if f in ['QDSUMS', 'SHA1SUMS']:
                continue

            if (not args.do_dot_files) and \
                    f[0] == '.':
                continue

            try:
                filestat = os.stat(ffn)
            except OSError, e:
                lg.debug(" --- cannot stat: %s (%d)", f, e.errno)
                if e.errno == errno.ENOENT:
                    #path does not exists - or is a broken symlink
                    continue
                else:
                    raise

            if minsize > 0:
                if filestat.st_size < minsize:
                    #lg.debug(" --- too small: %s (%d)", f, filestat.st_size)
                    continue

            print(ffn)

        if dirpath == '.' and args.no_recurse:
            break
