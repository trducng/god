"""
Commit the data for hashing
"""
import json
import queue
from pathlib import Path

import yaml

from god.utils.common import get_string_hash
from god.utils.exceptions import InvalidUserParams


def calculate_commit_hash(commit_obj):
    """Calculate hash commit from supplied information

    This method calculates the commit hash using the key, value in `commit_obj`. In
    case of `objects` key inside `commit_obj`, it calculates the hash value of
    `objects`, and then use that hash value instead of the dictionary when calculating
    hash of `commit_obj`.

    # Args:
        commit_obj <{}>: the commit object about to be dumped as commit

    # Returns:
        <str>: the hash value
    """
    str_rep = json.dumps(commit_obj, sort_keys=True)

    return get_string_hash(str_rep)


def read_commit(commit_id, commit_dir):
    """Read commit information

    # Args:
        commit_id <str>: commit id
        commit_dir <str|Path>: the path to commit

    # Returns:
        <{}>: commit information
    """
    path = Path(commit_dir, commit_id)
    with path.open("r") as f_in:
        commit_obj = yaml.safe_load(f_in)

    return commit_obj


def get_files_hashes_in_commit_dir(dir_id, commit_dirs_dir, prefix):
    """Get files and hashes in a commit

    # Args:
        dir_id <str>: commit id
        commit_dirs_dir <str|Path>: the path to dirs directory
        prefix <str>: the prefix to file

    # Returns:
        <{str: str}> fn and hashes
    """
    with Path(commit_dirs_dir, dir_id).open("r") as f_in:
        lines = f_in.read().splitlines()

    result = {}
    for each_line in lines:
        components = each_line.split(",")
        # @PRIORITY2: don't assume no ',' in filename -> more robust `dirs`
        fn = components[0]  # assume no ',' in filename
        if components[1] == "d":
            result.update(
                get_files_hashes_in_commit_dir(
                    dir_id=components[-1],
                    commit_dirs_dir=commit_dirs_dir,
                    prefix=str(Path(prefix, fn)),
                )
            )
        else:
            result[str(Path(prefix, fn))] = components[-1]

    return result


def get_files_hashes_in_commit(commit_id, commit_dir, commit_dirs_dir, plugin):
    """Get files and hashes in a commit

    # Args:
        commit_id <str>: commit id
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory

    # Returns:
        <{str: str}>: fn and hashes
    """
    commit_obj = read_commit(commit_id, commit_dir)
    # @PRIORITY1: use the correct commit
    result = get_files_hashes_in_commit_dir(
        commit_obj["tracks"][plugin], commit_dirs_dir, prefix="."
    )

    return result


def exists_in_commit(files, commit_id, commit_dir, commit_dirs_dir):
    """Check whether files exist in commit

    # Args:
        files <[str]>: list of relative path
        commit_id <str>: commit id
        commit_dir <str>: path to commit directory
        commit_dirs_dir <str>: path to commit dirs directory
    """
    if not files:
        return []

    files_hashes = get_files_hashes_in_commit(commit_id, commit_dir, commit_dirs_dir)

    exists = []
    for filepath in files:
        if filepath in files_hashes:
            exists.append(True)
        else:
            exists.append(False)

    return exists


def get_latest_parent_commit(commit1, commit2, commit_dir):
    """Get parrent commit of both commit1 and commit2

    # Args:
        commit1 <str>: the hash of commit 1
        commit2 <str>: the hash of commit 2
        commit_dir <str|Path>: the path to commit directory

    # Returns:
        <str>: commit id of parent, or None
    """
    to_check = queue.Queue()
    to_check.put(commit1)
    to_check.put(commit2)
    checked = set([])

    while not to_check.empty():
        commit_id = to_check.get()

        if commit_id is None:
            return

        if commit_id in checked:
            return commit_id

        checked.add(commit_id)
        commit_obj = read_commit(commit_id, commit_dir)
        if isinstance(commit_obj["prev"], (list, tuple)):
            for _ in commit_obj["prev"]:
                to_check.put(_)
        else:
            to_check.put(commit_obj["prev"])


def is_commit(start, commit_dir):
    """Check if there any commit starts with `start`

    # Args:
        start <str>: the starting pattern
        commit_dir <str|Path>: directory that stores commits

    # Returns:
        <str>: matched commit, else None, or raise if there are more than 1 match
    """
    result = []
    commits = [each.stem for each in Path(commit_dir).glob("*") if each.stem != "dirs"]
    for each in commits:
        if each.startswith(start):
            result.append(each)

    if len(result) > 1:
        raise InvalidUserParams(f"Ambiguous commits: {', '.join(result)}")
    elif len(result) == 1:
        return result[0]
