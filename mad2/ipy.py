
import re
import sys

from IPython.core.magic import (register_line_magic, register_cell_magic,
                                register_line_cell_magic)

from IPython.core.magic import magics_class, line_magic, cell_magic, Magics
from IPython.display import HTML

def error(*msg):
    sys.stderr.write(" ".join(map(str, msg)))

@register_line_magic
def mad_set(line):
    if not ' ' in line:
        error("invalid metadata set", line)
        return            
    k,v = line.strip().split(None, 1)
    if not re.match(r'^[A-Za-z][\w\.]+$', k):
        error("Invalid key:", k)
        return
    js = ["<script>"]
    js.append("var md = IPython.notebook.metadata;")
    js.append("if (!('mad' in md)) md.mad = {};")
    js.append("md.mad.{} = '{}';".format(k, v))
    js.append("</script>")
    return HTML(" ".join(js))

