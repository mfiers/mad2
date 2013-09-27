import os
import logging
import sys
import glob


# Put site-packages in front of sys.path so we don't end up importing the global
# readline.so
sys.path = (
    [p for p in sys.path if 'site-packages' in p] + \
    [p for p in sys.path if 'site-packages' not in p])

#hack - otherwise readline outputs anunwanted control
#character
if os.environ['TERM'] == 'xterm':
    os.environ['TERM'] = 'vt100'

import readline

lg =logging.getLogger(__name__)

################################################################################
##
## readline enabled user prompt
##
#########################################################################

## See if we can do intelligent things with job variables
def untangle(txt):
    print("untangle", txt)
    return txt
    #return sysConf.job.conf.interpret(txt)

## Handle mad directories
_FSCOMPLETECACHE = {}

def fsCompleter(text, state):
    def g(*a):
        pass
        #with open('/tmp/fscomp.%d.log' % os.getuid(), 'a') as F:
        #    F.write("\t".join(map(str, a)) + "\n")

    g("text   : ", text)
    g("state  :", state)

    if _FSCOMPLETECACHE and text in _FSCOMPLETECACHE.keys():
        try:
            #rv = _FSCOMPLETECACHE[text]
            #g(str(rv))
            rv = _FSCOMPLETECACHE[text][state]
            g('from cache', rv)
            return rv
        except:
            g('%s' % _FSCOMPLETECACHE)
            g('cache problem')
            import traceback
            E = traceback.format_exc()
            g(E)
            return None

    detangle = False
    utext = text
    #see if there are templates in the text - if so - untangle
    if '{{' in text or '{%' in text:
        #do a complete untangle first
        utext = untangle(text)
        g("untang  :", utext)
        detangle = True
        #if utext != text:
        #    detangle = True
    else:
        g("no detangle")

    g("utext : " + utext)

    #find the last word - to expand
    #string: stored in 'ctext'. The rest is in 'prefix'
    if ' ' in utext:
        addPrefix = True
        prefix, uptext = utext.rsplit(' ', 1)
    else:
        addPrefix = False
        prefix, uptext = "", utext

    g('prefix :', prefix)
    g('uptext :', uptext)

    if os.path.isdir(uptext) and not uptext[-1] == '/':
        sep = '/'
    else: sep = ''


    if prefix or uptext[:2] == './' or \
            uptext[:3] == '../' or uptext[0] == '/':
        #try to expand path
        #get all possibilities
        pos = glob.glob(uptext + sep + '*')
    else:
        pos = []

    np = []
    for i, p in enumerate(pos):
        g("found %s" % p)
        if os.path.isdir(p) and not p[-1] == '/':
            p += '/'
        if addPrefix:
            p = prefix + ' ' + p
        if detangle:
            g("detangling")
            g("from %s" % p)
            g("replacing %s" % utext)
            g("with %s" % text)
            p = p.replace(utext , text)
        np.append(p)
    g('pos', np)

    _FSCOMPLETECACHE[text] = np
    g(_FSCOMPLETECACHE)
    try:
        rv = np[state]
        return rv
    except IndexError:
        return None


def _get_mad_history_file(n):
    """
    return the history file for this parameter
    """
    histdir =  os.path.join(os.path.expanduser('~'), '.config', 'mad',
                            'history')
    if not os.path.exists(histdir):
        os.makedirs(histdir)
    histfile = os.path.join(histdir, n)
    return histfile

def _check_history_duplicates(value):
    # prevent duplicates in histtory the last ten lines
    # of the history - if the item is already there -
    # remove it again
    histlen = readline.get_current_history_length()
    if histlen > 1:
        for i in range(max(histlen-10, 0), histlen):
            previtem = readline.get_history_item(i)
            if previtem == value:
                lg.debug("removing duplicate %s" % value)
                readline.remove_history_item(histlen-1)
                break

def askUser(parameter, default="", data = {}, xtra_history=None):
    """
    :param parameter: paramter to ask value of
    :param default: default value - if absent use the last
      history item
    :param xtra_history: extra history file to show to the user
    """

    if not default:
        default = ""
    lg.debug("askUser {0} ({1})".format(parameter, default))

    def startup_hook():
        lg.debug("readline starup hook ({0})".format(default))
        readline.insert_text(str(default))
        #readline.redisplay()

    if xtra_history:
        history_file = xtra_history
    else:
        history_file = _get_mad_history_file(parameter)

    lg.debug("reading history from %s" % history_file)
    readline.clear_history()
    readline.set_completer_delims("\n`~!@#$^&*()-=+[]\|,?")
    readline.set_startup_hook(startup_hook)

    readline.set_completer(fsCompleter)
    readline.parse_and_bind("tab: complete")

    if history_file and os.path.exists(history_file):
        readline.read_history_file(history_file)

    try:
        vl = raw_input('%s: ' % parameter)
    finally:
        readline.set_startup_hook()

    if not xtra_history:
        # if we're note reading from an xtra history file -
        # save it to the parameter history
        _check_history_duplicates(vl)
        readline.write_history_file(history_file)
    else:
        #see if we want to write to an additional history file
        lg.debug("xtra history file processed - now do boring one")
        history_file = _get_mad_history_file(parameter)
        lg.debug("boring read %s" % history_file)
        readline.clear_history()
        if os.path.exists(history_file):
            readline.read_history_file(history_file)
        readline.add_history(vl)
        _check_history_duplicates(vl)

        readline.write_history_file(history_file)

    return vl

