import hashlib
import os
from pathlib import Path
import shutil

from god.base import settings


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


def construct_symlinks(paths, recursive=True):
    """Construct symlinks

    # Args
        paths <[str]>: list of relative paths
    """
    dir_obj = Path(settings.DIR_OBJ)

    if not isinstance(paths, (list, tuple)):
        paths = [paths]

    # collect non-symlinks
    files = []
    for each_path in paths:
        files += get_nonsymlinks(each_path, recursive=recursive)

    temp = {}
    for each_file in files:
        with open(each_file, 'rb') as f_in:
            file_hash = hashlib.sha256(f_in.read()).hexdigest()
            temp[str(each_file)] = file_hash

    # construct hash table
    hash_table = {}
    for each_file in files:
        # each_file = str(Path(each_file).resolve())
        with open(each_file, 'rb') as f_in:
            file_hash = hashlib.sha256(f_in.read()).hexdigest()
            hash_path = f'{file_hash[:2]}/{file_hash[2:4]}/{file_hash[4:]}'
        hash_path = dir_obj / hash_path
        hash_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(each_file, hash_path)
        sympath = Path(each_file)
        sympath.symlink_to(hash_path)
        hash_path.chmod(0o440)


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
    # directories, non_links = get_dir_detail(get_base_dir())
    import pdb; pdb.set_trace()

    # simplify some add and op operations with move
    # reverse_add_ops, reverse_remove_ops can dup since hashes are not unique
    reverse_add_ops = {value: key for key, value in add_ops.items()}
    reverse_remove_ops = {value: key for key, value in remove_ops.items()}
    add_hashes = set(add_ops.values())
    remove_hashes = set(remove_ops.values())
    move_hashes = list(add_hashes.intersection(remove_hashes))
    move = {}   # from: to
    for mh in move_hashes:
        fr = reverse_remove_ops[mh]
        to = reverse_add_ops[mh]
        remove_ops.pop(fr)
        add_ops.pop(to)
        move[fr] = to

    # move files
    for fr, to in move.items():
        shutil.move(Path(base_dir, fr), Path(base_dir, to))
