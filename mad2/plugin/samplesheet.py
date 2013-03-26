from __future__ import print_function

import logging
import sys
import re
import xlrd

import leip
from mad2.util import  get_filenames, get_all_mad_files

lg = logging.getLogger(__name__)
    

@leip.arg('file', nargs='*')
@leip.arg('samplesheet', help='sheet with information')
@leip.arg('--id', help='header of identifying column')
@leip.arg('--format', help='format string to identify file', 
          default='{0}')
@leip.arg('--sanitize', help='sanitize by removing special characters', 
          action='store_true')
@leip.arg('--multiple', help='multiple files per id allowed', 
          action='store_true')
@leip.arg('--apply', action='store_true', help='actually apply')
@leip.command
def samplesheet(app, args):
    """
    Apply samplesheet data
    """
    wb = xlrd.open_workbook(args.samplesheet)
    lg.debug("discovered sheets: %s" % ', '.join(wb.sheet_names()))
    sheetName = wb.sheet_names()[0]
    lg.debug("using sheet: '%s'" % sheetName)
    sheet = wb.sheet_by_name(sheetName)
    header = sheet.row_values(0)
    resanitize = re.compile(r'[^A-Za-z0-9]')
    def show_columns():
        print("Select from:")
        for col in header:
            print('- {0}'.format(col))
    
    if not args.id:
        print("Need a column with identifying information (--id)")
        show_columns()
        sys.exit(-1)

    if args.id in header:
        id_values = sheet.col_values(header.index(args.id), start_rowx=1)
        if args.sanitize:
            id_values = [resanitize.sub('', x) for x in id_values]
    else:
        print("Unknown column with identifying information (--id {0})".format(args.id))
        show_columns()
        sys.exit(-1)


    if len(id_values) != len(set(id_values)):
        print("duplicate ids")
        sys.exit(-1)

    lg.debug("id column: {0}".format(args.id))

    filenames = sorted(list(get_filenames(args)))

    #try to map values to files
    file2id = {}
    id2file = {}
    for fid in id_values:
        for filename in filenames:
                
            if args.sanitize:
                san_filename = resanitize.sub('', filename)
            else:
                san_filename = filename

            if args.format.format(fid) in san_filename:
                if filename in file2id:
                    print("filename to id clash: {0} - {1}/{2}".format(
                            fid, filename, file2id[filename]))
                    sys.exit(-1)
                if fid in id2file:
                    print("id to filename clash: {0} - {1}/{2}".format(
                            filename, fid, id2file[fid]))
                    sys.exit(-1)

                file2id[filename] = fid
                id2file[fid] = filename

    no_apply = 0
    if not args.apply:
        print("found mapping (apply with --apply):")
        for filename in filenames:
            fid = file2id.get(filename, '<none>')
            if fid != '<none>':
                no_apply += 1
            print(' - {0} : {1}'.format(fid, filename))
        if no_apply == 0:
            print("none applied - first few id values are:")
            print(", ".join(id_values[:5]))
        sys.exit()
    
    for madfile in get_all_mad_files(app, args):
        fid = file2id.get(madfile.filename)
        if fid in id_values:
            rowid = id_values.index(fid) + 1
            data = dict(zip(header, sheet.row_values(rowid)))
            madfile.mad.update(data)
            madfile.save()

                
    
    


            
