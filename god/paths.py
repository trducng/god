"""Utility to deal with paths, files and directories"""
import os
from collections import defaultdict
from pathlib import Path


def organize_files_by_prefix_with_tstamp(files, base_dir):
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
    files_dirs = defaultdict(list)

    for each_file in files:
        f = Path(each_file).resolve()
        parent = str(f.parent.relative_to(base_dir))
        if f.is_file():
            files_dirs[parent].append((f.name, f.stat().st_mtime))
        else:
            # the file is removed
            # TODO: doesn't make sense to handle the remove here because it requires
            # information inside the index to know a removed object is a file or,
            # folder, which this file shouldn't have access to index information
            # This file should strictly be about file / directory in folder
            files_dirs[parent].append((f.name, None))

    return result


def organize_files_in_dirs_by_prefix_with_tstamp(
    dirs, base_dir, files_dirs=None, recursive=True
):
    """Organize the files in directories into dictionary of files

    # Args:
        files <[str|Path]>: list of absolute values to file paths
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
        each_dir = Path(each_dir).resolve()

        for child in os.scandir(each_dir):

            if child.is_dir():
                # if child is folder, ignore or search recursively
                if child.name == '.god':
                    continue

                if recursive:
                    organize_files_in_dirs_by_prefix_with_tstamp(
                        child.path, base_dir, files_dirs=files_dirs, recursive=True
                    )
                continue

            # if child is a file
            if child.name == '.godconfig':
                continue

            parent = str(each_dir.relative_to(base_dir))
            files_dirs[parent].append((child.name, child.stat().st_mtime))

    return files_dirs

