
import copy
import os

from jinja2 import TemplateNotFound
from jinja2 import Environment, PackageLoader

import leip
from mad2.plugin import mongo
from mad2.ui import message

@leip.subparser
def report(app, args):
    """
    Report generation
    """
    pass


@leip.flag('-f', '--force')
@leip.arg('dir', nargs='?', default='.', help='location to generate the '
          'static html files' )
@leip.subcommand(report, "generate")
def report_generate(app, args):
    """
    Generate html reports
    """
    direc = os.path.expanduser(args.dir)

    env = Environment(
            loader=PackageLoader('mad2', 'template/report'))

    #make directory structure
    def mkdir(*args):
        p = os.path.join(*args)
        if not os.path.exists(p):
            os.makedirs(p)

    mkdir(direc, 'data')

    conf = app.conf['plugin.report']
    singletons = conf['singletons']

    data = {'singletons': singletons,
             'singleton': ''
            }

    def rendersave(name, tname=None, data={}, **kwargs):
        d = copy.copy(data)
        d.update(kwargs)

        message("creating %s" % name)
        if tname is None:
            template = env.get_template(name)
        else:
            template = env.get_template(tname)
        with open(os.path.join(args.dir, name), 'w') as F:
            F.write(template.render(d))

    rendersave('mad.css')
    print(data)
    rendersave('index.html', data=data)

    #generate singleton data
    for s in data['singletons']:
        message("getting data for %s", s)
        sumdata = mongo._single_sum(app, group_by=s, force=args.force)
        tsv_file = os.path.join(direc, 'data', 'single_{}.tsv'.format(s))
        with open(tsv_file, 'w') as F:
            F.write('name\tcount\tsum\n')
            for rec in sumdata:
                F.write('{_id}\t{count}\t{total}\n'.format(**rec))
            rendersave('single_{}.html'.format(s),
                       tname='singleton.html', data=data, singleton=s)




