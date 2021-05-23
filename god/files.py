import hashlib
import os
from pathlib import Path
import shutil

from god.base import get_base_dir, OBJ_DIR


def get_nonsymlinks(path, recursive=False):
    """Get non-symlink files in folder `path` (recursively)

    # Args
        path: the relative path to begin checking for files.
        recursive <bool>: find nonsymlinks recursively in sub-directories

    # Returns
        <[Paths]>: list of paths to non-symlink files
    """
    non_links = []
    for child in os.scandir(path):
        if child.is_symlink():
            continue

        if child.is_dir():
            if child.name == '.god':
                continue
            if recursive:
                non_links += get_nonsymlinks(child.path)
        else:
            non_links.append(child.path)

    return non_links


def construct_symlinks(paths):
    """Construct symlinks

    # Args
        paths <[str]>: list of relative paths
    """
    base_dir = get_base_dir()
    hash_dir = Path(base_dir, OBJ_DIR)

    if not isinstance(paths, (list, tuple)):
        paths = [paths]

    # collect non-symlinks
    files = []
    for each_path in paths:
        files += get_nonsymlinks(each_path)

    # construct hash table
    hash_table = {}
    for each_file in files:
        with open(each_file, 'rb') as f_in:
            file_hash = hashlib.sha256(f_in.read()).hexdigest()
            hash_path = f'{file_hash[:2]}/{file_hash[2:4]}/{file_hash[4:]}'
        hash_path = hash_dir / hash_path
        hash_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(each_file, hash_path)
        sympath = Path(each_file)
        sympath.symlink_to(hash_path)


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
    directories, non_links = get_dir_detail(get_base_dir())
    import pdb; pdb.set_trace()
