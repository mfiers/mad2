

import collections
import logging
from multiprocessing.dummy import Pool as ThreadPool
import os
import subprocess as sp
import sys
import uuid

import leip
import fantail
from mad2.util import get_all_mad_files
from mad2.recrender import recrender

lg = logging.getLogger(__name__)
#lg.setLevel(logging.DEBUG)


def get_template(conf, filetype, name):
    """
    get a template branch based on filetype
    """
    if not filetype is None:
        tloc = 'template.{}.{}'.format(filetype, name)
        if tloc in conf:
            return conf[tloc]

    tloc = 'template.default.{}'.format(name)
    return conf.get(tloc)


@leip.flag('-e', '--exit_error', help='exit on no template found')
@leip.arg('-g', '--group', help='group on', default=1)
@leip.arg('-t', '--group_template', help='group template')
@leip.arg('file', nargs='*')
@leip.arg('name', help='template name to render')
@leip.command
def render(app, args):
    """
    Render a template

    Render a template in the context of a single madfile.

    ---
    A template is a configuration subtree that has the
    following keys:
        - template
        - defaults (optional)

    templates are to be found at the following locations in the
    configuration

       - template.{{filetype}}.{{template_name}}
       - template.default.{{template_name}}

    """

    lg.debug("rendering key %s", args.name)


    groupby = args.group
    try:
        groupby = int(groupby)
        numeric_groups = True
    except:
        numeric_groups = False

    mf_generator = get_all_mad_files(app, args)

    if numeric_groups:
        for res in render_numeric(app, mf_generator, args.name, groupby,
                                args.exit_error):
            print(res)

        return

    lg.warning("not implemented yet (-g %s)", group)



def render_numeric(app,
                   mf_generator,
                   name, count,
                   exit_error = False):

    elements = []

    group_template = app.conf['plugin.template.group_template']
    group_count = 0
    group_defaults = {}
    for madfile in mf_generator:

        filetype = madfile.get('filetype', None)
        template = get_template(app.conf, filetype, name)

        if template is None:
            if exit_error:
                lg.warning("No template '%s' for file '%s'",
                           name, madfile['inputfile'])
                exit()
            else:
                continue


        if 'group' in template:
            if group_template is None:
                group_template = template['group']
            else:
                assert(group_template == template['group'])

        group_defaults.update(template['defaults'])

        #render an element
        elements.append(madfile.render(
                            template['template'],
                            [template['defaults'],
                            app.conf]).strip())


        if len(elements) == count:
            if group_template is None:
                group_template = app.conf['plugin.template.group_template']
            #render a group
            result = recrender(group_template,
                    [{'elements': elements,
                      'group_count': group_count},
                      group_defaults,
                      app.conf])
            yield result
            elements = []


    if len(elements) > 0:
        assert(not group_template is None)

        #render a group
        result = recrender(group_template,
                [{'elements': elements,
                  'group_count': group_count},
                  group_defaults,
                  app.conf])
        yield result

