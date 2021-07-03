"""
Commit the data for hashing
"""
import queue
from collections import defaultdict
from pathlib import Path

import yaml

from god.exceptions import InvalidUserParams
from god.files import get_string_hash
from god.index import Index


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
    keys = sorted(list(commit_obj.keys()))

    items = []
    for key in keys:
        if key == "objects":
            dir_hashes = commit_obj[key]
            dirs = sorted(list(commit_obj[key].keys()))
            obj_hash = get_string_hash("\n".join(f"{d},{dir_hashes[d]}" for d in dirs))
            items.append(f"{key},{obj_hash}")
        else:
            items.append(f"{key},{commit_obj[key]}")

    return get_string_hash("\n".join(items))


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


def get_files_hashes_in_commit_dir(dir_id, commit_dirs_dir, prefix=None):
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

    prefix = Path(".") if prefix is None else Path(prefix)
    result = {}
    for each_line in lines:
        components = each_line.split(",")
        fn = ",".join(components[:-1])
        result[str(Path(prefix, fn))] = components[-1]

    return result


def get_files_hashes_in_commit(commit_id, commit_dir, commit_dirs_dir):
    """Get files and hashes in a commit

    # Args:
        commit_id <str>: commit id
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory

    # Returns:
        <{str: str}>: fn and hashes
    """
    commit_obj = read_commit(commit_id, commit_dir)
    result = {}
    for prefix, dir_id in commit_obj["objects"].items():
        result.update(
            get_files_hashes_in_commit_dir(dir_id, commit_dirs_dir, prefix=prefix)
        )

    return result


def get_transform_operations(commit1, commit2, commit_dir, commit_dirs_dir):
    """Get add and remove operations to transform from state1 to state2

    The files from state1 to state2 are as follow:
        - Same path - Same hash -> Ignore
        - Same path - Different hash -> commit2 is added, commit1 is removed
        - Commit1 doesn't have - commit2 has -> Files are added
        - Commit1 has - Commit2 doesn't have -> Files are moved

    The output of this function serves:
        - file_add: add these files to the table in the new commit
        - file_remove: remove these files from the table in the new commit

    # Args
        commit1 <str>: the hash of commit 1. If None, this is the first time.
        commit2 <str>: the hash of commit 2.
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory

    # Returns
        <{}>: files newly added (recursively)
        <{}>: files newly removed (recursively)
    """
    files_hashes1 = (
        {}
        if commit1 is None
        else get_files_hashes_in_commit(commit1, commit_dir, commit_dirs_dir)
    )
    files_hashes2 = get_files_hashes_in_commit(commit2, commit_dir, commit_dirs_dir)
    fns1 = set(files_hashes1.keys())
    fns2 = set(files_hashes2.keys())

    add = {each: files_hashes2[each] for each in list(fns2.difference(fns1))}
    remove = {each: files_hashes1[each] for each in list(fns1.difference(fns2))}

    remain = list(fns2.intersection(fns1))
    for fn in remain:
        h1 = files_hashes1[fn]
        h2 = files_hashes2[fn]
        if h1 != h2:
            add[fn] = h2
            remove[fn] = h1

    return add, remove


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


def commit(user, email, message, prev_commit, index_path, commit_dir, commit_dirs_dir):
    """Commit from staging area

    # Args:
        user <str>: user name
        user_email <str>: user email address
        message <str>: commit message
        prev_commit <str>: previous commit id
        index_path <str>: path to index file
        commit_dir <str>: path to store commit
        commit_dirs_dir <str>: path to store commit info

    # Returns:
        <str>: the commit hash
    """
    with Index(index_path) as index:
        files_info = index.get_files_info(get_remove=False)

    index_files_dirs = defaultdict(list)
    for f in files_info:
        fp = Path(f[0])
        fh = f[2] or f[1]
        index_files_dirs[str(fp.parent)].append((fp.name, fh))

    dir_hashes = {}
    for d, fs in index_files_dirs.items():
        fs = sorted(fs, key=lambda obj: obj[0])
        fs = "\n".join(",".join(each) for each in fs)

        # get unique hash
        dir_hash = get_string_hash(fs)
        dir_hashes[d] = dir_hash

        # store dir commit
        commit_dir_file = Path(commit_dirs_dir, dir_hash)
        if commit_dir_file.is_file():
            continue
        with commit_dir_file.open("w") as f_out:
            f_out.write(fs)
        commit_dir_file.chmod(0o440)

    # construct commit object
    commit_obj = {
        "user": user,
        "email": email,
        "message": message,
        "prev": prev_commit,
        "objects": dir_hashes,
    }
    commit_hash = calculate_commit_hash(commit_obj)
    commit_file = Path(commit_dir, commit_hash)
    if commit_file.is_file():
        print("Commit already exists.")
        return commit_hash

    with commit_file.open("w") as f_out:
        yaml.dump(commit_obj, f_out)

    commit_file.chmod(0o440)

    # reconstruct index
    with Index(index_path) as index:
        files = [(_[0], _[2] or _[1], _[7]) for _ in files_info]
        index.construct_index_from_files_hashes_tsts(files)

    return commit_hash


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
