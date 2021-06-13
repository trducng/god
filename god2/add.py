"""
Commands:

    `god add folder`   -> add the folder to staging area, ready for commit
    --- This is just to mark what files to be calculated hash
"""

def add_files_from_files_dirs(files_dirs, index_path, base_dir):
    """Add the files to staging area

    This function handles add, update and removal of existing files

    # Args:
        files_dirs <{str: [str]}>: prefix - files, where prefix is a full relative path
            to BASE_DIR, while files include filename and timestamps only
        index_path <str>: path to index file
        base_dir <str>: project base directory
    """
    with Index(index_path) as index:
        for each_dir, files in files_dirs.items():
            add, update, remove = [], [], []

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


def add_dirs(dirs, index_path, base_dir):
    """Add the directory to staging area

    This function handles add, update and removal of existing directories.
    The operation is more complicated when there is removal, for example
    when running `god add folder1`:
        - folder1 has removed files
        - folder1/sub1 has removed files
        - folder1/sub1/sub1a has removed files
        - folder1/sub1 is removed
        - folder1/sub1/sub1a is removed and inside sub1a we have sub1a/sub1aa...

    # Args:
        dirs <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        base_dir <str>: project base directory
    """
    base_dir = Path(base_dir).resolve()
    if not isinstance(dirs, (list, int)):
        dirs = [dirs]

    removed_dirs = []
    existed_dirs = []
    for each_dir in dirs:
        each_dir = Path(each_dir).resolve()
        if each_dir.is_dir():
            existed_dirs.append(each_dir)
        else:
            removed_dirs.append(str(each_dir.relative_to(base_dir)))

    files_dirs = organize_files_in_dirs_by_prefix_with_tstamp(
        existed_dirs, base_dir, recursive=True
    )

    total_dirs = list(files_dirs.keys())
    root_dirs = []  # know root to quickly retrieve total sub-directories
    if "." in total_dirs:
        root_dirs = ["."]
    else:
        for each_dir in total_dirs:
            parents = list(Path(each_dir).parents)
            if len(parents) >= 2:
                root_dirs.append(str(parents[-2]))
    root_dirs = list(set(root_dirs))

    total_dirs = set(total_dirs)
    with Index(index_path) as index:
        # get remove directories (exist in `index` but not in real directory)
        for each_dir in root_dirs:
            removed_dirs += [
                each[0]
                for each in index.get_sub_directories(each_dir, recursive=True)
                if each[0] not in total_dirs
            ]
        index.update_dirs_tables(remove_dirs=removed_dirs)

        # get remove files
        for each_dir, files in files_dirs.items():
            dir_hash, dir_mhash = index.get_dir_hash(each_dir)
            dhash = dir_mhash or dir_hash
            if dhash:
                removed_files = index.get_files(
                    dhash, [each[0] for each in files], not_in=True
                )
                files_dirs[each_dir] += [(each[0], None) for each in removed_files]

    # make changes
    add_files_from_files_dirs(files_dirs, index_path, base_dir)


