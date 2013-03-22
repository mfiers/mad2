from __future__ import print_function

import leip
import logging
import xlrd

lg = logging.getLogger(__name__)
lg.setLevel(logging.DEBUG)
@leip.arg('file', nargs='*')
@leip.arg('samplesheet', help='sheet with information')
@leip.arg('--id', help='header of identifying column')
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

    if not args.id:
        print("Need a column with identifying information (--id)")
        print("Select from:")
        for col in header:
            print('- {}'.format(col))

    #print sheet
    #for rownum in range(sheet.nrows):
    #    print sheet.row_values(rownum)
