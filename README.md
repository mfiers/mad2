# Mad2

The poor man's system for tracking file metadata.

### What does Mad2 do?

It tracks file metadata. Basically - it creates a sidecar file for each file tracked. The sidecar file contains the metadata in YAML format, which means that it is easy to parse, edit & change (even manually).


### What?? Why?

 - Because I could not find anything remotely useful.
 - Most of the solutions are vastly overblown, I wanted something light & simple until something briljant comes along. We really need to start tracking all the files we're accumulating, and there seems to be no simple solution available.
 - Sidecar files because:
    - They are easy to copy along with the file (if they need to be copied()
    - They are platform and filesystem independent. They work across NFS and Samba. Unlike, for example, extended FS attributes.
    - Yaml because it is easy to edit manually, and can be parsed in any language. The information stored remains optimally accessible.

