"""Utility to deal with paths, files and directories"""
import os
from collections import defaultdict
from pathlib import Path


def resolve_paths(fds, base_dir):
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
    base_dir = Path(base_dir).resolve()
    files_dirs = defaultdict(list) if files_dirs is None else files_dirs

    for each_file in files:
        f = Path(each_file).resolve()
        parent = str(f.parent.relative_to(base_dir))
        if f.is_file():
            files_dirs[parent].append((f.name, f.stat().st_mtime))

    return files_dirs


def organize_files_in_dirs_by_prefix_with_tstamp(
    dirs, base_dir, files_dirs=None, recursive=True,
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
            if child.name == ".godconfig":
                continue

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


def filter_common_parents(fds):
    """Filter common parents in a list of files and directories

    # Args:
        fds <[str|Path]>: list of relative paths
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




if __name__ == "__main__":
    temp = [
        "path1/path2/path4",
        # 'path1',
        "path1/path2/path3",
        "path1/path2",
        "path3",
        ".",
    ]
    print(filter_common_parents(temp))
