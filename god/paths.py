"""Utility to deal with paths, files and directories"""
from collections import defaultdict
from pathlib import Path


def organize_files_by_prefix_with_tstamp(files):
    """Organize list of files into dictionary of files

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

    # Returns:
        <{str: [(str, flat)]}>: files_dirs format
    """
    files_dirs = defaultdict(list)
    for each_file in files:
        f = Path(each_file)
        if f.is_file():
            files_dirs[f.parent].append((f.name, f.stat().st_mtime))
        else:
            files_dirs[f.parent].append((f.name, None))

    return result

