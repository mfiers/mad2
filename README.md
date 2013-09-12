# Mad2

The poor man's system for tracking file metadata.

I guess that any research lab really needs to track all the files and data that they are accumulating. Mad2 is a system that allows you to do this with maximum flexibility, and a minimum demand on your system.

## So, what does Mad2 exactly do?

It tracks file metadata. Basically - it creates a sidecar file for each file tracked. The sidecar file contains the metadata in YAML format, which means that it is easy to parse, edit & change (even manually).

## What?? Why?

 - Because I could not find anything remotely useful. Most of the available solutions are vastly overblown or involve MS Excel. I wanted something light & simple until something really briljant comes along.
 - Sidecar files because:
    - They are easy to copy along with the file (if they need to be copied).
    - Central databases are difficult to keep in sync with the actual data. So, I wanted the information as close to the annotated file as possible.
    - They are platform and filesystem independent. Sidecar files work across NFS and Samba. Unlike, for example, extended FS attributes.
    - Yaml because it is easy to edit manually, and can be parsed in any language. The information stored remains optimally accessible.

## Installation

I'd recommend creating a virtual environment - but that is optional.

Install using (possibly using `sudo`):

    pip install Mad2


## Basic usage

All metadata is in the form of key/value pairs:

    mad set KEY VALUE *.txt

for example:

    mad set project dummy *.txt

but you could also have executed:

    ls *txt | mad set project dummy

which would also allow something more fancy:

    find -size +1M -name '*.bam' | mad set project dummy



