# Mad2

A poor man's system for tracking file metadata.

Many computational labs will needs to track files and data that they are accumulating, certainly if it is across a wide variety of different filetypes, directories, projects and types of workflows. Mad2 is a lightweight system that allows you to do this, retaining maximum flexibility.

Mad2 allows you to track file metadata, basically, it creates a hidden sidecar file for each file tracked (`.*.mad`). This sidecar contains the metadata in YAML format, which means that it is easy to parse, edit & change (even manually) - no vendor tie in.

## Why Mad2?

 - Because I could not find anything remotely useful. Most of the available software is vastly overblown or involves MS Excel. I wanted something light & simple, possibly until something brilliant comes along.
 - Sidecar files because:
    - They are easy to copy along with the file (if they need to be copied).
    - Central databases are difficult to keep in sync with actual data. So, I wanted the information as close to the annotated file as possible.
    - They are platform and file-system independent. Sidecar files work across NFS and Samba. Unlike, for example, extended FS attributes.
 - Yaml because it is easy to edit manually, and can be parsed in any language. The information stored remains optimally accessible.
 - I am well aware that sidecar files take up a lot of space, at least that of the system's block size.


## Why not Mad2?

 - Each sidecar file takes up space - so if you have many, many very small files, you will loose quite a lot of space annotating these files
 - If you copy files around a lot, it might become a hassle to copy the sidecars along with the original files.
 - If you have the capacity to install something more full fledged

## Installation

I'd recommend creating a virtual environment, and then install:

    mkvirtualenv mad
    pip install Mad2

Otherwise, you can install it globally:

    sudo pip install Mad2

## Basic usage

All metadata is in the form of key/value pairs:

    mad set KEY VALUE *.txt

for example:

    mad set project dummy *.txt

but you could also have executed:

    ls *txt | mad set project dummy

which would allow for something more fancy:

    find -size +1M -name '*.bam' | mad set project dummy

or to generate the sha1 checksums:

    find . -size +1M  | mad sha1

