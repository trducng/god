import hashlib
from bisect import bisect_left
from pathlib import Path

from god.db import get_files, get_sub_directory_and_hash, get_untouched_directories


def index(list_, element):
    """Locate the index of `element` in increasing sorted list `l`

    # Args
        l <[]>: an increasing sorted list
        element <>: an item in `l`

    # Returns
        <int>: the index of `element` in `l`. -1 if `element` not exist in `l`
    """
    i = bisect_left(list_, element)
    if i != len(list_) and list_[i] == element:
        return i
    return -1


def insert_path(files_hashes, path):
    """Insert the path in file name

    # Args
        file_hashes <[str, str]>: filename , file hash

    # Returns
        <[str, str]>: filename, file hash
    """
    return [(str(Path(path, fn)), fh) for fn, fh in files_hashes]


def get_transform_operations(state1, state2=None):
    """Get add and remove operations to transform from state1 to state2

    The files from state1 to state2 are as follow:
        - Same path - Same hash -> Ignore
        - Same path - Different hash -> State2 is added, state1 is removed
        - State1 doesn't have - State2 has -> Files are added
        - State1 has - State1 doesn't have -> Files are moved

    The output of this function serves:
        - file_add: add these files to the table in the new commit
        - file_remove: remove these files from the table in the new commit

    # Args
        state1 <str>: the hash of state1
        state2 <str>: the hash of state2. If None, this is the first time.

    # Returns
        <[]>: files newly added (recursively)
        <[]>: files newly removed (recursively)
    """
    file_add, file_remove = [], []

    if state2 is None:
        folders = get_untouched_directories([], db_name=state1)
        for folder_name, folder_hash in folders:
            each_files = [
                (str(Path(folder_name, name)), h)
                for (name, h) in get_files(folder_hash)
            ]
            file_add += each_files

        return file_add, file_remove

    state1_dirs_hashes = sorted(
        get_sub_directory_and_hash(".", recursive=True, db_name=state1),
        key=lambda obj: obj[0],
    )
    state2_dirs_hashes = sorted(
        get_sub_directory_and_hash(".", recursive=True, db_name=state2),
        key=lambda obj: obj[0],
    )

    # examine folders
    state1_dirs, state1_hashes = zip(*state1_dirs_hashes)
    visited_state1_indices = []
    for state2_dir, state2_hash in state2_dirs_hashes:
        idx = index(state1_dirs, state2_dir)
        if idx == -1:
            # dir exist in state2 but not in state1, add all files in state2 dir
            file_add += insert_path(get_files(state2_hash), state2_dir)
            continue

        if state1_hashes[idx] != state2_hash:
            # same directory, different content
            state1_files = insert_path(get_files(state1_hashes[idx]), state2_dir)
            state1_files = {fn: fh for fn, fh in state1_files}
            state2_files = insert_path(get_files(state2_hash), state2_dir)
            for state2_fn, state2_fh in state2_files:
                if state2_fn in state1_files:
                    if state2_fh != state1_files[state2_fn]:
                        # files updated
                        file_add.append((state2_fn, state2_fh))
                        file_remove.append((state2_fn, state1_files[state2_fn]))
                    state1_files.pop(state2_fn)
                else:
                    # new file
                    file_add.append((state2_fn, state2_fh))

            for state1_fn, state1_fh in state1_files.items():
                file_remove.append((state1_fn, state1_fh))

        visited_state1_indices.append(idx)

    # add remove files:
    unvisited_state1_indices = list(
        set(range(len(state1_dirs))).difference(visited_state1_indices)
    )
    for idx in unvisited_state1_indices:
        file_remove += insert_path(get_files(state1_hashes[idx]), state1_dirs[idx])

    return file_add, file_remove


def get_log_records(files, hashes):
    """Construct log records"""
    # @TODO: strip the BASE_DIR first
    out_records = [
        f"+{Path(each_file)} {each_hash}" for each_file, each_hash in zip(files, hashes)
    ]

    return out_records


def save_log(add_records, remove_records, log_dir):
    """Construct the logs based on add_records and remove_records

    The log has format:
        + file_path1 file_hash1
        - file_path2 file_hash1

    # Args
        add_records <[(str, str)]>: file path and hash
        remove_records <[(str, str)]>: file path and hash

    # Returns
        <str>: hash of the log files
    """
    add_records = [
        f"+ {file_path} {file_hash}" for (file_path, file_hash) in add_records
    ]
    remove_records = [
        f"- {file_path} {file_hash}" for (file_path, file_hash) in remove_records
    ]
    records = "\n".join(add_records + remove_records)
    hash_name = hashlib.sha256(records.encode()).hexdigest()

    with Path(log_dir, hash_name).open("w") as f_out:
        f_out.write(records)

    return hash_name


if __name__ == "__main__":
    # result = get_state_ops(".")
    file_add, file_remove = get_transform_operations(
        # "477ea9463b74aa740be85359ed69a1ab90f0b545bcc238d629b6bb76803e700d",
        # "4edd28d87b4223f086c6bb44c838082456eb7b5f97892311e5612aeb84fb9573",
        # "44ba89e3f3afa22482b4961b4480371b38d521b8cb1c08c350f763375c915a47",
        "ff7a72be8907be6dc50901db67baf268b1a784d8817a0021dbbaa8ca79cd362c",
        "11a7936355d055bc5437d9fc7f22926ee91fced3f947491d41655fac041d6e23",
    )
