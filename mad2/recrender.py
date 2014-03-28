"""
Render
"""

import logging
import re

import jinja2



def regex_sub(s, find, replace):
    """A non-optimal implementation of a regex filter"""
    return re.sub(find, replace, s)


def get_jinja_env():

    jenv = jinja2.Environment(
        undefined=jinja2.DebugUndefined)

    jenv.filters['re_sub'] = regex_sub
    return jenv


def recrender(template, data):

    jenv = get_jinja_env()
    rendered = template
    iteration = 0

    if not isinstance(data, list):
        data = [data]

#        data = fanttail.Fanstack
    # stack all data - to prevent potential problems
    # TODO: needs more investigation

    data_stacked = {}
    for d in data[::-1]:
        data_stacked.update(d)

    last = None
    while '{{' in rendered or '{%' in rendered:

        if iteration > 0 and rendered == last:
            # no improvement
            break
        last = rendered

        try:
            template = jenv.from_string(rendered)
        except:
            print("problem creating template with:")
            print(rendered)
            raise

        try:
            rendered = template.render(c=data_stacked, **data_stacked)
        except jinja2.exceptions.UndefinedError:
            pass
        except:
            print("cannot render")
            print(rendered)
            raise

        iteration += 1

    return rendered
