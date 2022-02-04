"""Contain instruction and template for file descriptor and directory descriptor.

Difference between storage and descriptor. Storage is substitutable, might be retrieved
on-demand. Descriptor plays important role in the logic of the code and the user
experience.

File
====

```
{
    "type": "file",
    "hash": "algorithm - e.g. sha256",
    "checksum": "original-checksum",
    "location": "location in storage",
    "plugins": [
        {"command": "tar"},
        {"command": "encryption", "key": "some-key"},
    ]
}
```

Directory
=========

```
{
    "type": "directory",
    "files": [
        ["filename","descriptor-hash","original-checksum"],
        ["filename","descriptor-hash","original-checksum"]
    ]
}
```

Commit entry
============

```
{
    "files": [
        ["dir1","descripto-hash1"],
        ["dir2","descripto-hash2"]
    ]
}
```
"""
from typing import Dict, List


class FileDescriptor:
    # @TODO: can create base class for descriptor content, similar to serializer idea
    @staticmethod
    def descriptor() -> Dict:
        return {
            "type": "file",
            "hash": "",
            "checksum": "",
            "location": "",
            "plugins": [],
        }


class DirectoryDescriptor:
    @staticmethod
    def descriptor() -> Dict:
        return {"type": "directory", "plugins": []}


class CommitDescriptor:
    @staticmethod
    def descriptor() -> List:
        return []
