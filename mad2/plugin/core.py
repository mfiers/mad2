import logging

import leip

import mad2.util

lg = logging.getLogger(__name__)

# make sure stores are cleaned up.
# note that initialization takes place in mad2.util - and only
# when a record is actually saved.
@leip.hook('finish')
def cleanup_stores(app):
    lg.debug("cleanup stores")
    mad2.util.cleanup_stores(app)


@leip.arg('file', nargs='*')
@leip.command
def echo(app, args):
    """
    Echo the filename

    note - this ensures that the sha1sum is calculated
    """
    for madfile in get_all_mad_files(app, args):
        print(madfile['inputfile'])


@leip.flag('-e', '--echo')
@leip.arg('file', nargs='*')
@leip.command
def save(app, args):
    """
    save the file to the mad datastore

    note - this ensures that the sha1sum is calculated
    """
    for madfile in get_all_mad_files(app, args):
        if madfile['orphan']:
            lg.warning("removing %s", madfile['inputfile'])
            lg.warning("sha1sum is/was: %s", madfile['sha1sum'])
        madfile.save()
        if args.echo:
            print madfile['inputfile']
