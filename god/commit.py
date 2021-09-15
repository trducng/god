"""
Commit the data for hashing
"""
from collections import defaultdict
from pathlib import Path

import yaml

from god.commits.base import calculate_commit_hash
from god.core.index import Index
from god.utils.common import get_string_hash


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
        records_info = index.get_records()

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

    records = []
    for rn, rh, rmh, rwh, remove in sorted(records_info, key=lambda obj: obj[0]):
        if remove:
            continue
        records.append((rn, rmh or rh, rwh))

    # construct commit object
    commit_obj = {
        "user": user,
        "email": email,
        "message": message,
        "prev": prev_commit,
        "objects": dir_hashes,
        "records": {rn: rh for rn, rh, _ in records if rh},
    }
    commit_hash = calculate_commit_hash(commit_obj)
    commit_file = Path(commit_dir, commit_hash)
    if commit_file.is_file():
        print("Commit already exists.")
        # TODO: require a stricter check for "objects" & "records" because the
        # commit hash will always be different because of diff in `prev_commit`
        return commit_hash

    with commit_file.open("w") as f_out:
        yaml.dump(commit_obj, f_out)

    commit_file.chmod(0o440)

    # reconstruct index
    with Index(index_path) as index:
        # files
        files = [(_[0], _[2] or _[1], _[7]) for _ in files_info]
        index.construct_index_from_files_hashes_tsts(files)

        # records
        index.reconstruct_records(records=records)

    return commit_hash
