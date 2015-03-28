
import logging
import os
import sys
import tempfile
import subprocess

from dateutil.parser import parse as dateparse
import leip
import fantail
from termcolor import cprint

from mad2.util import  get_mad_file, get_all_mad_files, get_filenames
from mad2.ui import message, error, errorexit
import mad2.ui

lg = logging.getLogger(__name__)


##
## define unset
##
@leip.arg('file', nargs="+")
@leip.arg('key')
@leip.arg('-e', '--echo', help='echo ')
@leip.command
def unset(app, args):
    """
    Remove a key/value from a file
    """
    lg.debug("unsetting: %s".format(args.key))
    key = args.key
    keyinfo = app.conf['keywords.{}'.format(key)]
    if keyinfo.get('cardinality', '1') == '+':
        errorexit("Not implemented - unsetting keys with cardinality > 1")

    for madfile in get_all_mad_files(app, args):
        #print(madfile)
        #print(madfile.mad.pretty())
        if args.key in madfile.mad:
            del(madfile.mad[args.key])

        madfile.save()

def _getkeyval(app, key, val, force):

    # First check if this will become a list.
    list_mode = False
    if key[0] == '+':
        lg.debug("treating {} as a list".format(key))
        list_mode = True
        key = key[1:]

    keyinfo = app.conf['keywords.{}'.format(key)]
    keytype = keyinfo.get('type', 'str')

    if (not force) and (not keyinfo.get('description')):
        errorexit('invalid key: "{0}" (use -f?)'.format(key))

    if list_mode and str(keyinfo.get('cardinality', '1')) == '1':
        errorexit("Cardinality == 1 - no lists!")
    elif keyinfo.get('cardinality', '1') == '+':
        list_mode = True

    if keytype == 'int':
        try:
            val = int(val)
        except ValueError:
            lg.error("Invalid integer: %s" % val)
            sys.exit(-1)
    elif keytype == 'float':
        try:
            val = float(val)
        except ValueError:
            lg.error("Invalid float: %s" % val)
            sys.exit(-1)
    elif keytype == 'boolean':
        if val.lower() in ['1', 'true', 't', 'yes', 'aye', 'y', 'yep']:
            val = True
        elif val.lower() in ['0', 'false', 'f', 'no', 'n', 'nope']:
            val = False
        else:
            lg.error("Invalid boolean: %s" % val)
            sys.exit(-1)
    elif keytype == 'date':
        try:
            val = dateparse(val)
        except ValueError:
            lg.error("Invalid date: %s" % val)
            sys.exit(-1)
        lg.debug("date interpreted as: %s" % val)

    if keytype == 'restricted':
        allowed = keyinfo['allowed']
        if not val in list(allowed.keys()):
            errorexit("Value '{0}' not allowed for key '{1}'".format(val, key))

    return key, val, list_mode

@leip.arg('file', nargs='*')
@leip.arg('-k', '--kv', help='key & value to set', metavar=('key', 'val'),
                nargs=2, action='append')
@leip.usage("usage: mad mset [-h] [-f] [-e] -k key val [[-k key val] ...] [file [file ...]]")
@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('-e', '--echo', action='store_true', help='echo filename')
@leip.command
def mset(app, args):
    """
    Set multiple key/value pairs.
    """
    all_kvs = []
    for k, v in args.kv:
        all_kvs.append(_getkeyval(app, k, v, args.force))

    for madfile in get_all_mad_files(app, args):
        print(madfile)
        for key, val, list_mode in all_kvs:
            if list_mode:
                if not key in madfile:
                    oldval = []
                else:
                    oldval = madfile[key]
                    if not isinstance(oldval, list):
                        oldval = [oldval]
                madfile.mad[key] = oldval + [val]

            else:
                #not listmode
                madfile.mad[key] = val

        if args.echo:
            print((madfile.filename))
        madfile.save()


@leip.flag('-t', '--transient', help='show transient keywords')
@leip.command
def keywords(app, args):
    """
    Show allowed keywords
    """
    maxkeylen = 0

    for key, keyinfo in app.conf['keywords'].items():
        if keyinfo['hide']:
            continue
        maxkeylen = max(maxkeylen, len(key))

    for key in sorted(app.conf['keywords'].keys()):
        keyinfo = app.conf['keywords'][key]
        transient = keyinfo['transient']
        if keyinfo['hide']:
            continue

        if transient and not args.transient:
            continue

        cprint(('{:' + str(maxkeylen) + '}').format(key), 'yellow',
               end=' : ')

        cprint(str(keyinfo['description']), end="")
        if transient:
            cprint(" (transient)", 'grey')
        else:
            print()
        if keyinfo['type'] == 'restricted':
            for i, allowed in enumerate(keyinfo['allowed']):
                ad = keyinfo['allowed'][allowed]
                if i == 0:
                    cprint(('  allowed: -'), end='')
                else:
                    cprint(('           -'), end='')
                cprint(('{}').format(allowed), "green", end=': ')
                cprint(ad)



@leip.arg('-d', '--editor', action='store_true', help='open an editor')
@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('-p', '--prompt', action='store_true', help='show a prompt')
@leip.arg('file', metavar='dir', nargs='?', default='.')
@leip.arg('value', help='value to set')
@leip.arg('key', help='key to set')
@leip.command
def dset(app, args):
    """
    Like set, but at the directory level
    """
    args.dir = True
    args.echo = False
    madset(app, args)


@leip.arg('-f', '--force', action='store_true', help='apply force')
@leip.arg('-p', '--prompt', action='store_true', help='show a prompt')
@leip.arg('-D', '--dir', action='store_true', help='dir modus - write ' +
          'to ./mad.config files to be read for all files in the dir ' +
          '(and below)')
@leip.arg('-d', '--editor', action='store_true', help='open an editor')
@leip.arg('-e', '--echo', action='store_true', help='echo filename')
@leip.arg('file', nargs='*')
@leip.arg('value', help='value to set', nargs='?')
@leip.arg('key', help='key to set')
@leip.commandName('set')
def madset(app, args):
    """
    Set a key/value for one or more files.

    Use this command to set a key value pair for one or more files.

    This command can take the following forms::

        mad set project test genome.fasta
        ls *.fasta | mad set project test
        find . -size +10k | mad set project test

    """

    key = args.key
    val = args.value


    if args.prompt or args.editor:
        if not args.value is None:
            # when asking for a prompt - the next item on sys.argv
            # is assumed to be a file, and needs to be pushed
            # into args.file
            args.file = [args.value] + args.file

    madfiles = []

    #gather all madfiles for later parsing
    use_stdin = not (args.prompt or args.editor)
    if args.dir:
        for m in get_filenames(args, use_stdin, allow_dirs=True):
            if not os.path.isdir(m):
                continue

            fn = os.path.join(m, 'mad.config')
            mf = fantail.Fantail()
            if os.path.exists(fn):
                mf = fantail.yaml_file_loader(fn)
            mf._mad_dir_name = m
            mf._mad_file_name = fn
            madfiles.append(mf)
    else:
        for m in get_all_mad_files(app, args, use_stdin):
            madfiles.append(m)

    #check if mad needs to show a prompt or editor
    if val is None and not (args.prompt or args.editor):
        args.prompt = True

    # show prompt or editor
    if args.prompt or args.editor:
        # get a value from the user

        default = ''
        #Show a prompt asking for a value
        data = madfiles[0]
        default = madfiles[0].get(key, "")

        if args.prompt:
            sys.stdin = open('/dev/tty')
            val = mad2.ui.askUser(key, default, data)
            sys.stdin = sys.__stdin__

        elif args.editor:
            editor = os.environ.get('EDITOR','vim')
            tmp_file = tempfile.NamedTemporaryFile('wb', delete=False)

            #write default value to the tmp file
            if default:
                tmp_file.write(default + "\n")
            else:
                tmp_file.write("\n")
            tmp_file.close()

            tty = open('/dev/tty')

            subprocess.call('{} {}'.format(editor, tmp_file.name),
                stdin=tty, shell=True)
            sys.stdin = sys.__stdin__


            #read value back in
            with open(tmp_file.name, 'r') as F:
                #removing trailing space
                val = F.read().rstrip()
            #remove tmp file
            os.unlink(tmp_file.name)

    #process key & val
    key, val, list_mode = _getkeyval(app, key, val, args.force)

    # Now process madfiles
    lg.debug("processing %d files" % len(madfiles))


    for madfile in madfiles:
        if list_mode:
            if not key in madfile:
                oldval = []
            else:
                oldval = madfile.get(key)
                if not isinstance(oldval, list):
                    oldval = [oldval]
            if args.dir:
                madfile[key] = oldval + [val]
            else:
                madfile.mad[key] = oldval + [val]
        else:
            #not listmode
            if args.dir:
                madfile[key] = val
            else:
                madfile.mad[key] = val

        if args.echo:
            if args.dir:
                print((madfile._mad_dir_name))
            else:
                print((madfile['filename']))

        if args.dir:
            fantail.yaml_file_save(madfile, madfile._mad_file_name)
        else:
            madfile.save()
