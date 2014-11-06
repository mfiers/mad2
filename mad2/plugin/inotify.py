
import fnmatch
import logging
import os

import leip

import mad2.util

lg = logging.getLogger(__name__)
lg.setLevel(logging.DEBUG)


@leip.subparser
def inotify(app, args):
    """ Inotify fs monitoring
    """
    pass


@leip.arg('-s', '--minsize', help='minimum size for a file to be considered', 
          type=int, default=100)
@leip.arg('path')
@leip.subcommand(inotify, 'watch')
def inotify_watch(app, args):
    
    import pyinotify

    parent_paths_to_ignore = []

    def must_be_saved(path):

        if os.path.isdir(path):
            return False

        base = os.path.basename(path)
        for pattern in app.conf['ignore.filename']:
            if fnmatch.fnmatch(base, pattern):
                #lg.debug("ignoring (base) %s", path)
                return False

        thisdir = os.path.dirname(path).rstrip('/')

        while True:
            thisbase = os.path.basename(thisdir)
            if thisdir in parent_paths_to_ignore:
                #lg.debug("Ignoring path cache: %s", path)
                return False

            if thisbase in  app.conf['ignore.inpath']:
                parent_paths_to_ignore.append(thisdir)
                return False

            for pp in app.conf['ignore.parentpattern']:
                pptest = os.path.join(thisdir, pp)
                if os.path.exists(pptest):
                    #lg.debug("Ignoring: %s because of %s", path, pptest)
                    parent_paths_to_ignore.append(thisdir)
                    return False

            thisdir = os.path.dirname(thisdir).rstrip('/')
            if not thisdir:
                break
                                  
        return True

        

    class MadEventHandler(pyinotify.ProcessEvent):

        def process_IN_DELETE(self, event):
            lg.info("Delete: %s", event.pathname)
            maf = mad2.util.get_mad_file(app, event.pathname)
            maf.delete()
            maf.flush()

        def process_IN_MOVED_FROM(self, event):
            return self.process_IN_DELETE(event)

        def process_default(self, event):

            pn = event.pathname

            try:
                if os.path.exists(pn) and \
                   os.path.getsize(pn) < args.minsize:
                    return
                
                if not must_be_saved(pn):
                    return 

            except Exception as e:
                lg.error("problem processing %s", pn)
                lg.error(" - %s", str(e))
                return

            try:
                lg.debug("saving: %s", event.pathname)
                maf = mad2.util.get_mad_file(app, event.pathname)
                maf.save()
                maf.flush()
            except Exception as e:
                lg.error("problem mad saving %s", pn)
                lg.error(" - %s", str(e))
                

    mask = pyinotify.IN_DELETE \
        | pyinotify.IN_MOVED_FROM \
        | pyinotify.IN_CREATE \
        | pyinotify.IN_MODIFY \
        | pyinotify.IN_ATTRIB \
        | pyinotify.IN_MOVED_TO 


    wm = pyinotify.WatchManager()

    handler = MadEventHandler()
    notifier = pyinotify.Notifier(wm, handler)
    wm.add_watch(args.path, mask, rec=True)
    notifier.loop()


