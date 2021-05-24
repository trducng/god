"""
Commit the data for hashing
"""
import hashlib
import os
import sqlite3
from multiprocessing import Process, Pool
from pathlib import Path
import shutil

from god.base import change_index, settings
from god.db import (
    get_directory_hash, get_sub_directory, get_file_hash, get_removed_files,
    is_directory_maintained, get_connection_cursor, create_directory_db,
    create_index_db, get_untouched_directories)
from god.files import get_dir_detail, get_hash, construct_symlinks, get_nonsymlinks
from god.logs import get_log_records, save_log


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
            file_hash = str(Path(original).relative_to(settings.DIR_OBJ)).replace('/', '')
            rel_path = str(file_path.relative_to(settings.DIR_BASE))
            files.append((rel_path, file_hash))
            continue

        if child.is_dir():
            if child.name == '.god':
                continue

            rel_path = str(Path(child.path).relative_to(settings.DIR_BASE))
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
            rel_path = str(file_path.relative_to(settings.DIR_BASE))
            files.append((rel_path, file_hash))

    # populate directory_remove
    sub_dir = get_sub_directory(Path(dir_name).relative_to(settings.DIR_BASE), recursive=True)
    directory_remove = [
        each for each in sub_dir
        if each not in
        directory_remain + directory_add + [str(Path(dir_name).relative_to(settings.DIR_BASE))]]

    # populate file_add
    dhash = get_directory_hash(Path(dir_name).relative_to(settings.DIR_BASE))
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
            (str(Path(dir_name, file_name).relative_to(settings.DIR_BASE)), file_hash)
            for (file_name, file_hash) in file_remove]

    con.close()

    return (
            directory_add, directory_remove, directory_remain,
            file_add, file_remove, file_remain
    )


def commit(path):
    """Commit from path

    # @TODO: currently support path as directory. Will need to support file
    """
    path = str(Path(path).resolve())

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

        remainings += [str(Path(settings.DIR_BASE, each)) for each in directory_add]
        idx += 1

    # construct index table
    remainings = [str(Path(path).relative_to(settings.DIR_BASE)) for path in remainings]
    temp_ = remainings + directory_removes
    other_remainings = get_untouched_directories(temp_)
    records = list(zip(remainings, db_hashes))
    records += other_remainings
    commit_hash = create_index_db(records)

    # change pointer
    change_index(commit_hash)

    # construct symlinks
    construct_symlinks(directory_adds + directory_remains, recursive=True)
    construct_symlinks(path, recursive=False)

    return (
            directory_adds, directory_removes, directory_remains,
            file_adds, file_removes, file_remains)


def play_with_setting():
    print(settings)

if __name__ == '__main__':
    path = Path('/home/john/datasets/god-test/type1').resolve()
    (
        directory_add, directory_remove, directory_remain,
        file_add, file_remove, file_remain
    ) = commit(path)
