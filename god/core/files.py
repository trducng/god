import hashlib
import os
import shutil
from collections import defaultdict
from pathlib import Path
from typing import List

from god.core.conf import settings


def get_file_hash(file_):
    """Calculate file hash"""
    with open(file_, "rb") as f_in:
        file_hash = hashlib.sha256(f_in.read()).hexdigest()

    return file_hash


def copy_objects_with_hashes(files, dir_obj, base_dir):
    """Construct symlinks

    # Args
        files <[(str, str)]>: list of relative paths and hash value
        dir_obj <str>: the path to object directory
        base_dir <str>: the path to base directory
    """
    dir_obj = Path(dir_obj).resolve()
    base_dir = Path(base_dir).resolve()

    if not isinstance(files, (list, tuple)):
        files = [files]

    # construct hash table
    for fn, fh in files:
        fn = Path(base_dir, fn)
        hash_path = f"{fh[:2]}/{fh[2:4]}/{fh[4:]}"
        hash_path = dir_obj / hash_path
        hash_path.parent.mkdir(parents=True, exist_ok=True)
        if hash_path.is_file():
            continue
        shutil.copy(fn, hash_path)
        hash_path.chmod(0o440)


def copy_hashed_objects_to_files(files, dir_obj, base_dir):
    """Copy hashed objects to files

    # Args
        files <[(str, str)]>: list of relative paths and hash value
        dir_obj <str>: the path to object directory
        base_dir <str>: the path to base directory
    """
    dir_obj = Path(dir_obj).resolve()
    base_dir = Path(base_dir).resolve()

    for fn, fh in files:
        fn = Path(base_dir, fn)
        fn.parent.mkdir(parents=True, exist_ok=True)
        hash_path = f"{fh[:2]}/{fh[2:4]}/{fh[4:]}"
        hash_path = dir_obj / hash_path

        shutil.copy(hash_path, fn)
        fn.chmod(0o664)


def get_objects_tst(objects, dir_obj):
    """Get objects timestamp

    # Args:
        objects <[str]>: list of object hashes
        dir_obj <str>: the path to object directory
    """
    tsts = []
    for ob in objects:
        path = Path(dir_obj, f"{ob[:2]}/{ob[2:4]}/{ob[4:]}")
        tsts.append(path.stat().st_mtime)
    return tsts


def get_files_tst(files, base_dir):
    """Get files timestamps

    # Args:
        files <[str|Path]>: list of file paths relative to `base_dir`
        base_dir <str|Path>: project base directory

    # Returns:
        <[float]>: list of timestamps
    """
    tsts = []
    for f in files:
        path = Path(base_dir, f)
        tsts.append(path.stat().st_mtime)
    return tsts


def resolve_paths(fds, base_dir) -> List[str]:
    """Resolve path relative to `base_dir`

    # Args:
        fds <[str]>: list of absolute paths
        base_dir <str|Path>: the repo base directory

    # Returns
        <[str]>: list of relative path to `base_dir`
    """
    base_dir = Path(base_dir).resolve()
    return [str(Path(each).resolve().relative_to(base_dir)) for each in fds]


def organize_files_by_prefix_with_tstamp(files, base_dir, files_dirs=None):
    """Organize list of files into dictionary of files

    The prefix is the directory containing files, relative to `BASE_DIR`

    # Example:
        >> files = ['dir1/file1', 'dir1/file2', 'dir2/file3']
        >> files_dirs = organize_files_by_prefix(files)
        >> print(file_dirs)
        {
            'dir1': [('file1' timestamp1), ('file2', timestamp2)],
            'dir2': [('file3', timestamp3)]
        }

    # Args:
        files <[str|Path]>: list of absolute values to file paths
        base_dir <str|Path>: the repo base directory

    # Returns:
        <{str: [(str, float)]}>: files_dirs format
    """
    files_dirs = defaultdict(list) if files_dirs is None else files_dirs

    for each_file in files:
        f = Path(base_dir, each_file)
        parent = str(f.parent.relative_to(base_dir))
        if f.is_file():
            files_dirs[parent].append((f.name, f.stat().st_mtime))

    return files_dirs


def organize_files_in_dirs_by_prefix_with_tstamp(
    dirs,
    base_dir,
    files_dirs=None,
    recursive=True,
):
    """Organize the files in directories into dictionary of files

    # Args:
        dirs <[str|Path]>: list of absolute values to directory path
        base_dir <str|Path>: the repo base directory
        dirs_dirs <{str: [(str, float)]>: the place to store result
        recursive <bool>: whether to look for files in directory recursively

    # Returns:
        <{str: [(str, flat)]}>: files_dirs format
    """
    base_dir = Path(base_dir).resolve()
    files_dirs = defaultdict(list) if files_dirs is None else files_dirs

    if not isinstance(dirs, (list, int)):
        dirs = [dirs]

    for each_dir in dirs:
        each_dir = Path(base_dir, each_dir).resolve()

        for child in os.scandir(each_dir):

            if child.is_dir():
                # if child is folder, ignore or search recursively
                if child.name == ".god":
                    continue

                if recursive:
                    organize_files_in_dirs_by_prefix_with_tstamp(
                        child.path, base_dir, files_dirs=files_dirs, recursive=True
                    )
                    continue

            # if child is a file
            parent = str(each_dir.relative_to(base_dir))
            files_dirs[parent].append((child.name, child.stat().st_mtime))

    return files_dirs


def separate_paths_to_files_dirs(fds, base_dir):
    """Separate files and directories out of each other

    # Args:
        fds <[str|Path]>: list of relative paths

    # Returns:
        <[str]>: list of files
        <[str]>: list of directories
    """
    files = []
    dirs = []
    unknowns = []

    for fd in fds:
        fd = Path(base_dir, fd).resolve()
        if fd.is_dir():
            dirs.append(str(fd.relative_to(base_dir)))
            continue
        if fd.is_file():
            files.append(str(fd.relative_to(base_dir)))
            continue
        unknowns.append(str(fd.relative_to(base_dir)))

    if "." in dirs:
        return [], ["."], []

    return files, dirs, unknowns


def filter_common_parents(fds: List[str]):
    """Filter common parents in a list of files and directories

    # Args:
        fds: list of relative paths
    """
    fds = sorted(fds, key=lambda obj: len(obj))

    if "." in fds:
        return ["."]

    matches = set()
    for fd in fds:
        parents = set([str(_) for _ in Path(fd).parents][:-1])
        if parents.intersection(matches):
            continue
        matches.add(fd)

    return sorted(list(matches), key=lambda obj: len(obj))


def retrieve_files_info(files, dirs, base_dir):
    """Retrieve file info from a list of paths (paths can be directory or file)

    # Args
        files <[str]>: list of absolute path
        dirs <[str]>: list of absolute path
        base_dir <str|Path>: the repo base directory

    # Returns
        <{str: [(str, float)]>: files, dirs format
    """
    files_dirs = organize_files_by_prefix_with_tstamp(files, base_dir)
    files_dirs = organize_files_in_dirs_by_prefix_with_tstamp(
        dirs, base_dir, files_dirs=files_dirs, recursive=True
    )

    return files_dirs


def get_nonsymlinks(path, recursive=False):
    """Get non-symlink files in folder `path` (recursively)

    OLD FROM HERE.

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
            if child.name == ".god":
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
        with open(each_file, "rb") as f_in:
            file_hash = hashlib.sha256(f_in.read()).hexdigest()
            temp[str(each_file)] = file_hash

    # construct hash table
    for each_file in files:
        # each_file = str(Path(each_file).resolve())
        with open(each_file, "rb") as f_in:
            file_hash = hashlib.sha256(f_in.read()).hexdigest()
            hash_path = f"{file_hash[:2]}/{file_hash[2:4]}/{file_hash[4:]}"
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
            if child.name == ".god":
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
        with open(each_file, "rb") as f_in:
            file_hash = hashlib.sha256(f_in.read()).hexdigest()
            hashes.append(file_hash)

    return hashes


def compare_files_states(state1, state2):
    """Get transform operations from state1 to state2

    # Args:
        state1 <{fn: fh}>: list of file path and hashes
        state2 <{fn: fh}>: list of file path and hashes

    # Returns:
        <{fn: fh}>: files to be added
        <{fn: fh}>: files to be removed
    """
    fns1 = set(state1.keys())
    fns2 = set(state2.keys())

    add = {each: state2[each] for each in list(fns2.difference(fns1))}
    remove = {each: state1[each] for each in list(fns1.difference(fns2))}

    remain = list(fns2.intersection(fns1))
    for fn in remain:
        h1 = state1[fn]
        h2 = state2[fn]
        if h1 != h2:
            add[fn] = h2
            remove[fn] = h1

    return add, remove
