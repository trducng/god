"""
Commit the data for hashing
"""
from collections import defaultdict
import hashlib
import os
import sqlite3
from multiprocessing import Process, Pool
from pathlib import Path
import shutil

import yaml

from god.base import change_index, settings
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
        if key == 'objects':
            dir_hashes = commit_obj[key]
            dirs = sorted(list(commit_obj[key].keys()))
            obj_hash = get_string_hash('\n'.join(f"{d},{dir_hashes[d]}" for d in dirs))
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
    with path.open('r') as f_in:
        commit_obj = yaml.safe_load(f_in)

    return commit_obj


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
        files_info = index.get_files_info(remove=False)

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
        with commit_dir_file.open('w') as f_out:
            f_out.write(fs)
        commit_dir_file.chmod(0o440)

    # construct commit object
    commit_obj = {
        "user": user,
        "email": email,
        "message": message,
        "prev": prev_commit,
        "objects": dir_hashes
    }
    commit_hash = calculate_commit_hash(commit_obj)
    commit_file = Path(commit_dir, commit_hash)
    if commit_file.is_file():
        print('Nothing changed, exit')
        return

    with commit_file.open('w') as f_out:
        yaml.dump(commit_obj, f_out)

    commit_file.chmod(0o440)

    # reconstruct index
    with Index(index_path) as index:
        files = [(_[0], _[2] or _[1], _[7]) for _ in files_info]
        index.construct_index_from_files_hashes_tsts(files)

    return commit_hash


if __name__ == '__main__':
    commit(
        user="johntd54",
        email="trungduc1992@gmail.com",
        message="Initial commit",
        prev_commit="",
        index_path='/home/john/datasets/dogs-cats/.god/index',
        commit_dir='/data/datasets/dogs-cats/.god/commits',
        commit_dirs_dir='/data/datasets/dogs-cats/.god/commits/dirs'
    )
