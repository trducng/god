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
from db import get_directory_hash
from files import get_dir_detail, get_hash
from logs import get_log_records


def get_nonsymlinks(root):
    """Get non-symlink files in folder `root` (recursively)

    # Args
        root: the path to begin checking for files.

    # Returns
        <[Paths]>: list of paths to non-symlink files
    """
    non_links = []
    for child in os.scandir(root):
        if child.is_symlink():
            continue

        if child.is_dir():
            if child.name == '.god':
                continue
            non_links += get_nonsymlinks(child)
        else:
            non_links.append(child.path)

    return non_links


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

    # Args
        dir_name <str>: the path of the directory

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

    for child in os.scandir(dir_name):
        if child.is_symlink():
            # TODO calculate the hash
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
            file_path = Path(child.path)
            with file_path.open('rb') as f_in:
                file_hash = hashlib.sha256(f_in.read()).hexdigest()
            rel_path = str(file_path.relative_to(BASE_DIR))
            files.append((rel_path, file_path))

    for each_file in files:




def commit(path=None):
    """Commit from path

    # @TODO: currently support path as directory. Will need to support file
    """
    if path is None:
        path = BASE_DIR

    con = sqlite3.connect(DB_DIR, MAIN_DB)
    cur = con.cursor()

    if is_table_exists(str(path.relative_to(BASE_DIR)), cur):
        # check if the timestamp still okie, if the timestamp is not okie, 
        pass
    else: # this is a new thing, add to log

        # get directories and files
        directories, files = get_dir_detail(path)   # the directory here is for check timestamp
        hashes = get_hash(files)
        log_records = get_log_records(files, hashes)
        save_log(log_records)

        # create table

        # add record to dir table
        pass


if __name__ == '__main__':
    commit()
    # read_db()
