import os
from pathlib import Path
from typing import List, Set, Tuple

from god.index.base import Index
from god.index.utils import column_index
from god.plugins.base import plugin_endpoints


def collapse_directory_status_add(add: List) -> Tuple[List, Set]:
    """Collapse directory for the status add

    We will collapse files in "add" into a parent directory when:
        - files not in root directory (i.e. "." as parent)
        - there aren't any directory with the same name in index

    Args:
        add: list if added items, each item is [name, hash, timestamp]

    Returns:
        Similar to add [name, hash, timestamp]. But if the name is a folder, then
        hash will be empty string, and timestamp will be 0
    """
    result, visited, collapse = [], set([]), set([])

    with Index(index_path=plugin_endpoints("files")["index"]) as index:
        for name, hash_, timestamp in add:
            parent = str(Path(name).parent)
            if parent == ".":
                result.append([name, hash_, timestamp])
                continue
            if parent in collapse:
                continue
            if parent in visited:
                result.append([name, hash_, timestamp])
                continue

            visited.add(parent)
            if index.get_folder(names=[parent], get_remove=False, conflict=False):
                result.append([name, hash_, timestamp])
                continue

            collapse.add(parent)
            result.append([f"{parent}{os.sep}", "", 0])

    return result, visited


def collapse_directory_status_remove(remove: List[str]) -> List[str]:
    """Collapse directory for the status remove

    We will collapse files in "remove" into a parent directory when:
        - files not in root directory (i.e. "." as parent)
        - there aren't any directory with the same name in the working directory

    Args:
        remove: list if removed items, each item is "name"

    Returns:
        Similar to remove: [name]
    """
    tracks_dir = Path(plugin_endpoints("files")["tracks"])
    result, visited, collapse = [], set([]), set([])

    for name in remove:
        parent = str(Path(name).parent)
        if parent == ".":
            result.append(name)
        if parent in collapse:
            continue
        if parent in visited:
            result.append(name)
            continue

        visited.add(parent)
        if (tracks_dir / parent).is_dir():
            result.append(name)
            continue

        collapse.add(parent)
        result.append(f"{parent}{os.sep}")

    return result


def collapse_directory_status_stage_add(
    stage_add: List[str], add: Set[str]
) -> List[str]:
    """Collapse directory for the status stage_add

    We will collapse files in "stage_add" into a parent directory when:
        - files not in root directory (i.e. "." as parent)
        - there aren't extra files in the parent directory that are not in stage_add
        - there are files in that directory that has "hash" value (mean already there
            before)

    Args:
        stage_add: list if stage added items, each item is "name"
        add: set of directories in "add"

    Returns:
        Similar to stage_add [name]
    """
    result, visited, collapse = [], set([]), set([])

    with Index(index_path=plugin_endpoints("files")["index"]) as index:
        for name in stage_add:
            parent = str(Path(name).parent)
            if parent == ".":
                result.append(name)
                continue
            if parent in collapse:
                continue
            if parent in visited:
                result.append(name)
                continue

            visited.add(parent)
            if parent in add:
                result.append(name)
                continue
            files = [
                each
                for each in index.get_folder(
                    names=[parent], get_remove=False, conflict=False
                )
                if each[column_index("hash")]
            ]
            if files:
                result.append([name])
                continue

            collapse.add(parent)
            result.append(f"{parent}{os.sep}")

    return result


def poststatus(file_status):
    """Collapse files into parent folder for add, remove, stage_add and stage_remove

    Args:
        file_status: the file status, they are: stage_add, stage_update, stage_remove,
            add, update, remove, reset_timestamp, unset_mhash

    Returns:
        - * stage_add
        - stage_update
        - stage_remove
        - * add
        - update
        - *remove
        - reset_timestamp
        - unset_mhash
    """
    add, add_visited = collapse_directory_status_add(file_status[3])
    stage_add = collapse_directory_status_stage_add(file_status[0], add_visited)
    return [
        stage_add,
        file_status[1],
        file_status[2],
        add,
        file_status[4],
        collapse_directory_status_remove(file_status[5]),
        file_status[6],
        file_status[7],
    ]
