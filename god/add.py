"""Add operation"""
import uuid
from collections import defaultdict
from pathlib import Path

from god.files import get_file_hash
from god.index import Index
from god.paths import organize_files_by_prefix_with_tstamp


def add_files(files_dirs, index_path, base_dir):
    """Add the files to staging area

    This function handles add and update of existing files

    # Args:
        files_dirs <{str: [str]}>: prefix - files, where prefix is a full relative path
            to BASE_DIR, while files include filename and timestamps only
        index_path <str>: path to index file
        base_dir <str>: project base directory
    """
    with Index(index_path) as index:
        for each_dir, files in files_dirs.items():
            add, update,remove = [], [], []

            dir_hash, dir_mhash = index.get_dir_hash(each_dir)
            new_dir_tst = Path(base_dir, each_dir).stat().st_mtime

            dhash = dir_mhash or dir_hash
            if not dhash:
                # everything in here is new
                for fn, tst in files:
                    add.append((fn, get_file_hash(Path(base_dir, each_dir, fn)), tst))

                index.create_files_table(add, each_dir, new_dir_tst, modified=True)
                continue

            current_files, _ = zip(*files)
            indexed_files = index.get_files(dhash, files=current_files)
            indexed_files = {each[0]: each[1:] for each in indexed_files}

            for fn, tst in files:
                if fn not in indexed_files:
                    add.append((fn, get_file_hash(Path(base_dir, each_dir, fn)), tst))
                    continue

                fp = Path(base_dir, each_dir, fn)
                if fp.is_file():
                    if tst == indexed_files[fn][2]:
                        # equal timestamp
                        continue

                    fh = get_file_hash(fp)
                    if fh == indexed_files[fn][0] or fh == indexed_files[fn][1]:
                        # equal hash
                        continue

                    update.append((fn, fh, tst))
                else:
                    # file is removed
                    remove.append(fn)

            index.update_files_tables(add, update, remove, each_dir, new_dir_tst)

if __name__ == '__main__':
    # files_dirs = {
    #     'folder1': [('file1', 123312.31231), ('file2', 121231.1111)],
    #     'folder2': [('file3', 12312312.12312)]
    # }
    # add_files(
    #         files_dirs,
    #         '/home/john/temp/add_god/index',
    #         '/home/john/temp/add_god')

    files_dirs = {
        'folder1': [('file1', 123399.31231), ('file2', None), ('file3', 121232.1111)],
    }
    add_files(
            files_dirs,
            '/home/john/temp/add_god/index1',
            '/home/john/temp/add_god/update')
