"""
Commit the data for hashing
"""
import hashlib
import os
import sqlite3
from multiprocessing import Process, Pool
from pathlib import Path
import shutil

from constants import BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR, DB_DIR, MAIN_DB
from db import (
    get_directory_hash, get_sub_directory, get_file_hash, get_removed_files,
    is_directory_maintained, get_connection_cursor, create_directory_db,
    create_index_db, get_untouched_directories)
from files import get_dir_detail, get_hash
from logs import get_log_records
from history import change_index


def commit_add():

    # Collect files
    files = get_nonsymlinks(BASE_DIR)

    # Calculate hash
    hash_table = {}
    for each_file in files:
        with open(each_file, 'rb') as f_in:
            file_hash = hashlib.sha256(f_in.read()).hexdigest()
            hash_path = f'{file_hash[:2]}/{file_hash[2:4]}/{file_hash[4:]}'
            hash_table[each_file] = hash_path

    # Record the add
    add_records = []
    for src, dest in hash_table.items():
        dest = Path(HASH_DIR, dest)
        if dest.is_file():      # NOTE: files can be moved here,
                                # or can be unchanged here after unlock
            continue
        add_records.append((src, dest))

    # Save the records
    out_file = Path(MAIN_DIR, 'temp_record')
    out_records = [
        f'+{Path(src).relative_to(BASE_DIR)} {hash_table[src].replace("/", "")}'
        for src, _ in add_records]
    with out_file.open('w') as f_out:
        f_out.write('\n'.join(out_records))
    with out_file.open('rb') as f_in:
        hash_name = hashlib.sha256(f_in.read()).hexdigest()
    shutil.move(out_file, Path(out_file.parent, hash_name))

    # Move objects to hash directory
    for src, dest in add_records:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(src, dest)

    # Create symlink
    for src, dest in add_records:
        sympath = Path(src)
        sympath.symlink_to(dest)


def check_directory(dir_name):
    """Check the content of directory

    The output of this function serves:
        - directory_add: follow up on these directories
        - directory_remove: remove these sub-directories out of the new commit
        - directory_remain: keep these sub-directories in the new commit
        - file_add: add these files to the table in the new commit
        - file_remove: remove these files from the table in the new commit
        - file_remain: keep these files to the table in the new commit

    # Args
        dir_name <str>: the absolute path of the directory

    # Returns
        <[]>: sub-directory newly added or modified
        <[]>: sub-directory newly removed
        <[]>: sub-directories that stay the same
        <[]>: files newly added
        <[]>: files newly removed
        <[]>: files that stay the same
    """
    directory_add, directory_remove, directory_remain = [], [], []
    file_add, file_remove, file_remain = [], [], []
    files = []      # aggregate files because they are both symlink + files

    # get detail of each child item
    # populate directory_add & directory_remain
    for child in os.scandir(dir_name):
        if child.is_symlink():
            # get the symlink hash
            file_path = Path(child.path)
            original = file_path.resolve()
            file_hash = str(Path(original).relative_to(HASH_DIR)).replace('/', '')
            rel_path = str(file_path.relative_to(BASE_DIR))
            files.append((rel_path, file_hash))
            continue

        if child.is_dir():
            if child.name == '.god':
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
            with file_path.open('rb') as f_in:
                file_hash = hashlib.sha256(f_in.read()).hexdigest()
            rel_path = str(file_path.relative_to(BASE_DIR))
            files.append((rel_path, file_hash))

    # populate directory_remove
    sub_dir = get_sub_directory(Path(dir_name).relative_to(BASE_DIR))
    directory_remove = [
        each for each in sub_dir if each not in directory_remain]

    # populate file_add
    dhash = get_directory_hash(Path(dir_name).relative_to(BASE_DIR))
    if not dhash:
        file_add = files
        return (
                directory_add, directory_remove, directory_remain,
                file_add, file_remove, file_remain
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
            for (file_name, file_hash) in file_remove]
    con.commit()

    return (
            directory_add, directory_remove, directory_remain,
            file_add, file_remove, file_remain
    )


def commit(path=None):
    """Commit from path

    # @TODO: currently support path as directory. Will need to support file
    """
    if path is None:
        path = BASE_DIR

    directory_adds, directory_removes, directory_remains = [], [], []
    file_adds, file_removes, file_remains = [], [], []
    db_hashes = []
    remainings = [path]

    # get the files and folders recursively
    idx = 0
    while idx < len(remainings):
        (
            directory_add, directory_remove, directory_remain,
            file_add, file_remove, file_remain
        ) = check_directory(remainings[idx])
        directory_adds += directory_add
        directory_removes += directory_remove
        directory_remains += directory_remain
        file_adds += file_add
        file_removes += file_remove
        file_remains += file_remain

        # create the DB
        db_hashes.append(create_directory_db(file_add + file_remain))

        remainings += [str(Path(BASE_DIR, each)) for each in directory_add]
        idx += 1

    # construct index table
    temp_ = remainings + directory_removes + directory_remains
    temp_ = [str(Path(path).relative_to(BASE_DIR)) for each in temp_]
    other_remainings = get_untouched_directories(temp_)
    other_remainings = [(str(Path(BASE_DIR, path)), dh) for (path, dh) in other_remainings]
    records = list(zip(remainings, db_hashes))
    records += other_remainings
    commit_hash = create_index_db(records)

    # move files to symlinks

    # change pointer
    change_index(commit_hash)

    return (
            directory_adds, directory_removes, directory_remains,
            file_adds, file_removes, file_remains)


if __name__ == '__main__':
    # commit()
    # read_db()
    (
        directory_add, directory_remove, directory_remain,
        file_add, file_remove, file_remain
    ) = commit('/home/john/datasets/dogs-cats')
    import pdb; pdb.set_trace()
