from copy import deepcopy
from pathlib import Path
from typing import List

from god.records.constants import RECORDS_INTERNALS, RECORDS_LEAVES, RECORDS_ROOT
from god.records.storage import get_internal_nodes, get_leaf_nodes
from god.records.utils import list_records


def prestatus(names: List[str]) -> List[str]:
    """Prestatus - use correct internal paths

    Example:
        >> print(prestatus(["record-name-1", "record-name-2"]))
        ["record-name-1/root", "record-name-2/root"]

    Args:
        names: list of records names, supplied by user. Assume to infer all records
            names if contains "." or does not contain anything

    Returns:
        Corrected internal paths to check for status
    """
    if not names or "." in names:
        names = list_records()

    return [f"{each}/root" for each in names]


def poststatus(file_status: List) -> List:
    """Clean up the filename in status

    Example:
        >> poststatus([["record-name-1/root"], [],...])
        ["record-name-1"], [],...]

    Args:
        file_status: the file status, they are: stage_add, stage_update, stage_remove,
            add, update, remove, reset_timestamp, unset_mhash

    Returns:
        Cleaned `file_status`
    """
    result = []
    for status in file_status:
        new_status = deepcopy(status)
        for idx, f in enumerate(new_status):
            if isinstance(f, list):
                f[0] = f[0].split("/")[0]
            elif isinstance(f, str):
                new_status[idx] = f.split("/")[0]
        result.append(new_status)

    return result


def preadd(base_dir: str):
    """Organize records internal and leaf folders to contain only relevant files

    Essentially, this method get files that are not reachable by the root nodes and
    remove them. For example:

        Before:
        .
        └── records-name-1/
            ├── root
            ├── internals/
            │   ├── internal-hash1
            │   ├── internal-hash2
            │   ├── internal-hash3
            │   └── ...
            └── leaves/
                ├── leaf-hash1
                ├── leaf-hash2
                ├── leaf-hash3
                └── ...

        After: from `root` hash, clean up internals/ and leaves/ folders
        .
        └── records-name-1/
            ├── root
            ├── internals/
            │   ├── internal-hash1
            │   ├── internal-hash3
            │   └── ...
            └── leaves/
                ├── leaf-hash1
                ├── leaf-hash3
                └── ...

    Args:
        base_dir: the directory that contains interested records
    """
    path = Path(base_dir)
    with (path / RECORDS_ROOT).open("r") as fi:
        root_hash = fi.read()

    idir = path / RECORDS_INTERNALS
    ldir = path / RECORDS_LEAVES

    # remove irrelevant internal nodes
    internal_nodes = get_internal_nodes(root_hash, str(idir))
    not_needed = set([_.name for _ in idir.glob("*")]).difference(internal_nodes)
    for each in list(not_needed):
        (idir / each).unlink()

    # remove irrelevant leaf nodes
    leaf_nodes = [each[0] for each in get_leaf_nodes(root_hash, str(idir))]
    not_needed = set([_.name for _ in ldir.glob("*")]).difference(leaf_nodes)
    for each in list(not_needed):
        (ldir / each).unlink()
