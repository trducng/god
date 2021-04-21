import hashlib
import os
from pathlib import Path

from constants import BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR, DB_DIR, MAIN_DB


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


def get_dir_detail(dir_name):
    """Get dir detail recursively

    # Returns
        <[(str, timestamp)]>: name of sub-directories and timestamps
        <[str]>: filenames
    """
    directories = []
    non_links = []
    for child in os.scandir(dir_name):
        if child.is_symlink():
            continue

        if child.is_dir():
            if child.name == '.god':
                continue
            directories.append((child.path, child.stat().st_mtime))
            sub_dirs, sub_files = get_dir_detail(child.path)
            directories += sub_dirs
            non_links += sub_files
        else:
            non_links.append(child.path)

    return directories, non_links


def get_hash(files):
    """Construct the hash of files"""
    hashes = []
    for each_file in files:
        with open(each_file, 'rb') as f_in:
            file_hash = hashlib.sha256(f_in.read()).hexdigest()
            hashes.append(file_hash)

    return hashes

if __name__ == '__main__':
    directories, non_links = get_dir_detail(BASE_DIR)
    import pdb; pdb.set_trace()
