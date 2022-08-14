import os
from pathlib import Path
from typing import List, Set, Tuple

from god.core.files import remove_subpaths
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
    endpoints = plugin_endpoints("files")
    current_dir = str(Path.cwd().resolve().relative_to(endpoints["tracks"]))

    result, visited, collapse = [], set([]), set([])
    with Index(index_path=endpoints["index"]) as index:
        for name, hash_, timestamp in add:
            parent = str(Path(name).parent)
            if parent == ".":
                result.append([os.path.relpath(name, current_dir), hash_, timestamp])
                continue
            if parent in collapse:
                continue
            if parent in visited:
                result.append([os.path.relpath(name, current_dir), hash_, timestamp])
                continue

            visited.add(parent)
            if index.get_folder(names=[parent], get_remove=False, conflict=False):
                result.append([os.path.relpath(name, current_dir), hash_, timestamp])
                continue

            collapse.add(parent)

    for each_dir in remove_subpaths(list(collapse)):
        result.append([f"{os.path.relpath(each_dir, current_dir)}{os.sep}", "", 0])

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
    current_dir = str(Path.cwd().resolve().relative_to(tracks_dir))

    result, visited, collapse = [], set([]), set([])

    for name in remove:
        parent = str(Path(name).parent)
        if parent == ".":
            result.append(os.path.relpath(name, current_dir))
        if parent in collapse:
            continue
        if parent in visited:
            result.append(os.path.relpath(name, current_dir))
            continue

        visited.add(parent)
        if (tracks_dir / parent).is_dir():
            result.append(os.path.relpath(name, current_dir))
            continue

        collapse.add(parent)

    for each_dir in remove_subpaths(list(collapse)):
        result.append(f"{os.path.relpath(each_dir, current_dir)}{os.sep}")

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
    endpoints = plugin_endpoints("files")
    current_dir = str(Path.cwd().resolve().relative_to(endpoints["tracks"]))

    result, visited, collapse = [], set([]), set([])

    with Index(index_path=endpoints["index"]) as index:
        for name in stage_add:
            parent = str(Path(name).parent)
            if parent == ".":
                result.append(os.path.relpath(name, current_dir))
                continue
            if parent in collapse:
                continue
            if parent in visited:
                result.append(os.path.relpath(name, current_dir))
                continue

            visited.add(parent)
            if parent in add:
                result.append(os.path.relpath(name, current_dir))
                continue
            files = [
                each
                for each in index.get_folder(
                    names=[parent], get_remove=False, conflict=False
                )
                if each[column_index("hash")]
            ]
            if files:
                result.append(os.path.relpath(name, current_dir))
                continue

            collapse.add(parent)

    for each_dir in remove_subpaths(list(collapse)):
        result.append(f"{os.path.relpath(each_dir, current_dir)}{os.sep}")

    return result


def poststatus(file_status):
    """Post-process the files status for easier viewing.

    The post-process:
        - Collapse files into parent folder for add, remove, and stage_add.
        - Show the file/directory paths relative to current working directory

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
