import hashlib
from bisect import bisect_left
from pathlib import Path

from constants import BASE_DIR, LOG_DIR
from db import get_sub_directory_and_hash, get_untouched_directories, get_files



def index(l, element):
    """Locate the index of `element` in increasing sorted list `l`

    # Args
        l <[]>: an increasing sorted list
        element <>: an item in `l`

    # Returns
        <int>: the index of `element` in `l`. -1 if `element` not exist in `l`
    """
    i = bisect_left(l, element)
    if i != len(l) and l[i] == element:
        return i
    return -1

def get_transform_operations(state1, state2):
    """Get add and remove operations to transform from state1 to state2

    The output of this function serves:
        - file_add: add these files to the table in the new commit
        - file_remove: remove these files from the table in the new commit
        - file_remain: keep these files to the table in the new commit

    # Args
        state1 <str>: the hash of state1
        state2 <str>: the hash of state2

    # Returns
        <[]>: files newly added (recursively)
        <[]>: files newly removed (recursively)
        <[]>: files that stay the same
    """
    directory_add, directory_remove, directory_remain = [], [], []
    file_add, file_remove, file_remain = [], [], []
    files = []  # aggregate files because they are both symlink + files

    state1_dirs_hashes = sorted(
        get_sub_directory_and_hash(".", state1), key=lambda obj: obj[0]
    )
    state2_dirs_hashes = sorted(
        get_sub_directory_and_hash(".", state2), key=lambda obj: obj[0]
    )

    # examine folders
    state1_dirs, state1_hashes = **zip(state1_dirs_hashes)
    visited_state1_indices = []
    for state2_dir, state2_hash in state2_dirs_hashes:
        idx = index(state1_dirs, state2_dir)
        if idx != -1:

        if state1_hashes[idx] == state2_hash:
            # unchanged directory, nothing to worry about
            continue
        else:
            # same directory, different content
    except ValueError:
        # state2_dir not in state1_dirs
        pass

    # get detail of each child item
    # populate directory_add & directory_remain
    for child in os.scandir(dir_name):
        if child.is_symlink():
            # get the symlink hash
            file_path = Path(child.path)
            original = file_path.resolve()
            file_hash = str(Path(original).relative_to(HASH_DIR)).replace("/", "")
            rel_path = str(file_path.relative_to(BASE_DIR))
            files.append((rel_path, file_hash))
            continue

        if child.is_dir():
            if child.name == ".god":
                continue

            rel_path = str(Path(child.path).relative_to(BASE_DIR))
            dhash = get_directory_hash(rel_path)
            if not dhash:
                directory_add.append(rel_path)
                continue

            same = is_directory_maintained(rel_path, child.stat().st_mtime)
            if same:
                directory_remain.append(rel_path)
                continue

            directory_add.append(rel_path)
        else:
            # calculate hash
            file_path = Path(child.path)
            with file_path.open("rb") as f_in:
                file_hash = hashlib.sha256(f_in.read()).hexdigest()
            rel_path = str(file_path.relative_to(BASE_DIR))
            files.append((rel_path, file_hash))

    # populate directory_remove
    sub_dir = get_sub_directory(Path(dir_name).relative_to(BASE_DIR), recursive=True)
    directory_remove = [
        each
        for each in sub_dir
        if each
        not in directory_remain
        + directory_add
        + [str(Path(dir_name).relative_to(BASE_DIR))]
    ]

    # populate file_add
    dhash = get_directory_hash(Path(dir_name).relative_to(BASE_DIR))
    if not dhash:
        file_add = files
        return (
            directory_add,
            directory_remove,
            directory_remain,
            file_add,
            file_remove,
            file_remain,
        )

    # populate file_add and file_remain
    con, cur = get_connection_cursor(dhash)
    for file_path, file_hash in files:
        file_db_hash = get_file_hash(Path(file_path).name, cur)
        if not file_db_hash:
            file_add.append((file_path, file_hash))
            continue
        if file_db_hash == file_hash:
            file_remain.append((file_path, file_hash))
        else:
            file_add.append((file_path, file_hash))
            # file_remove.append((file_path, file_db_hash))

    # populate file_remove
    exist = [str(Path(fp).name) for (fp, fh) in file_remain]
    file_remove = get_removed_files(exist, cur)
    file_remove = [
        (str(Path(dir_name, file_name).relative_to(BASE_DIR)), file_hash)
        for (file_name, file_hash) in file_remove
    ]
    con.commit()

    return (
        directory_add,
        directory_remove,
        directory_remain,
        file_add,
        file_remove,
        file_remain,
    )


def get_state_ops(state):
    """Get the state operations"""
    result = []
    folders = get_untouched_directories([])
    for folder_name, folder_hash in folders:
        each_files = [
            (str(Path(folder_name, name)), h) for (name, h) in get_files(folder_hash)
        ]
        result += each_files

    return result


def get_log_records(files, hashes):
    """Construct log records"""
    out_records = [
        f"+{Path(each_file).relative_to(BASE_DIR)} {each_hash}"
        for each_file, each_hash in zip(files, hashes)
    ]

    return out_records


def save_log(add_records, remove_records):
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

    with Path(LOG_DIR, hash_name).open("w") as f_out:
        f_out.write(records)

    return hash_name


if __name__ == "__main__":
    result = get_state_ops(".")
    import pdb

    pdb.set_trace()
