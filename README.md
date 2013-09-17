# Mad2

A poor man's system for tracking file metadata.

I guess that any computational lab needs to track the files and data that they are accumulating, certainly if it is accross a wide variety of different projects. Mad2 is a system that allows you to do this with maximum flexibility and minimal demands.

Mad2 allows you to track file metadata, basically, it creates a sidecar file for each file tracked (`.*.mad`). This sidecar contains the metadata in YAML format, which means that it is easy to parse, edit & change (even manually).

## Why Mad2?

 - Because I could not find anything remotely useful. Most of the available software is vastly overblown or involves MS Excel. I wanted something light & simple until something really briljant comes along.
 - Sidecar files because:
    - They are easy to copy along with the file (if they need to be copied).
    - Central databases are difficult to keep in sync with actual data. So, I wanted the information as close to the annotated file as possible.
    - They are platform and filesystem independent. Sidecar files work across NFS and Samba. Unlike, for example, extended FS attributes.
 - Yaml because it is easy to edit manually, and can be parsed in any language. The information stored remains optimally accessible.
 - I am well aware that sidecar files take up a lot of space, at least that of the system's block size.

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

which would allow for something more fancy:

    find -size +1M -name '*.bam' | mad set project dummy



