from collections import defaultdict
from pathlib import Path

from god.core.files import (
    filter_common_parents,
    get_file_hash,
    resolve_paths,
    retrieve_files_info,
    separate_paths_to_files_dirs,
)
from god.index.base import Index


def track_staging_changes(fds, index_path, base_dir):
    """Track staging changes

    # Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        base_dir <str>: project base directory

    # Returns:
        <[str]>: add - list of added files
        <[str]>: update - list of updated files
        <[str]>: remove - list of removed files
    """
    base_dir = Path(base_dir).resolve()
    if not isinstance(fds, (list, int)):
        fds = [fds]

    fds = resolve_paths(fds, base_dir)
    fds = filter_common_parents(fds)  # list of relative paths to `base_dir`

    add, update, remove = [], [], []

    with Index(index_path) as index:
        for fd in fds:
            result = index.get_folder(names=[fd], get_remove=True)
            for entry in result:
                if entry[3]:  # marked as removed
                    if entry[1]:
                        remove.append(entry[0])
                elif entry[2]:
                    if entry[1] is None:
                        add.append(entry[0])
                    elif entry[2] != entry[1]:
                        update.append(entry[0])

    return add, update, remove


def track_working_changes(fds, index_path, base_dir):
    """Track changes from working area compared to staging and commit area

    This function handles add, update and removal of existing files
    and directories.
    The operation is more complicated when there is removal, for example
    when running `god add folder1`:
        - folder1 has removed files
        - folder1/sub1 has removed files
        - folder1/sub1/sub1a has removed files
        - folder1/sub1 is removed
        - folder1/sub1/sub1a is removed and inside sub1a we have sub1a/sub1aa...

    Also, items specific in fds can both exist and removed.

    # Args:
        fds <str>: the directory to track (absolute path)
        index_path <str>: path to index file
        base_dir <str>: project base directory

    # Returns
        <[str, str, float]>: add - files newly added
        <[str, str, float]>: update - files updated
        <[str]>: remove - files removed
        <[str, float]>: reset_tst - files that changed in timestamp but same content
        <[str]>: files that are changed, and then manually changed back to commit ver
    """
    base_dir = Path(base_dir).resolve()
    if not isinstance(fds, (list, tuple)):
        fds = [fds]

    fds = resolve_paths(fds, base_dir)  # list of relative directory paths to `base_dir`
    fds = filter_common_parents(fds)  # list of relative directory paths to `base_dir`

    files, dirs, unknowns = separate_paths_to_files_dirs(fds, base_dir)
    files_dirs = retrieve_files_info(files, dirs, base_dir)

    index_files_dirs, index_unknowns = defaultdict(list), []
    with Index(index_path) as index:
        for fd in fds:
            result = index.get_folder(names=[fd], get_remove=False)
            if not result:  # not exist
                index_unknowns.append(fd)
                continue
            if result[0][0] == fd:  # single file
                index_files_dirs[str(Path(fd).parent)].append(result[0])
                continue
            for _ in result:  # directory of files
                index_files_dirs[str(Path(_[0]).parent)].append(
                    (Path(_[0]).name, _[1], _[2], _[3], _[4], _[5])
                )

        add, update, remove, reset_tst, unset_mhash = [], [], [], [], []
        dirs = set(files_dirs.keys())
        dirs_idx = set(index_files_dirs.keys())

        add_dirs = list(dirs.difference(dirs_idx))
        remove_dirs = list(dirs_idx.difference(dirs))
        remain_dirs = list(dirs.intersection(dirs_idx))

        for each_dir in add_dirs:
            for fn, tst in files_dirs[each_dir]:
                add.append(
                    (
                        str(Path(each_dir, fn)),
                        get_file_hash(Path(base_dir, each_dir, fn)),
                        tst,
                    )
                )

        for each_dir in remove_dirs:
            for _ in remove_dirs[each_dir]:
                remove.append(str(Path(each_dir, _[0])))

        for each_dir in remain_dirs:
            path_files = {fn: tst for fn, tst in files_dirs[each_dir]}
            index_files = {each[0]: each[1:] for each in index_files_dirs[each_dir]}
            pfn = set(path_files.keys())
            ifn = set(index_files.keys())

            # add operation
            for fn in list(pfn.difference(ifn)):
                add.append(
                    (
                        str(Path(each_dir, fn)),
                        get_file_hash(Path(base_dir, each_dir, fn)),
                        path_files[fn],
                    )
                )

            # remove operation
            for fn in list(ifn.difference(pfn)):
                remove.append(str(Path(each_dir, fn)))

            # update operation
            for fn in list(pfn.intersection(ifn)):
                if path_files[fn] == index_files[fn][4]:
                    # equal timestamp
                    continue

                fh = get_file_hash(Path(base_dir, each_dir, fn))
                if fh == index_files[fn][1]:
                    # equal modified file hash
                    reset_tst.append((str(Path(each_dir, fn)), path_files[fn]))
                    continue

                if fh == index_files[fn][0]:
                    reset_tst.append((str(Path(each_dir, fn)), path_files[fn]))
                    if index_files[fn][1]:
                        # reset to commit, update the timestamp
                        unset_mhash.append(str(Path(each_dir, fn)))
                    continue

                update.append((str(Path(each_dir, fn)), fh, path_files[fn]))

    return add, update, remove, reset_tst, unset_mhash


def track_files(fds, index_path, base_dir):
    """Track statuses of the directories

    # Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        base_dir <str>: project base directory
    """
    add, update, remove, reset_tst, unset_mhash = track_working_changes(
        fds, index_path, base_dir
    )
    stage_add, stage_update, stage_remove = track_staging_changes(
        fds, index_path, base_dir
    )

    return (
        stage_add,
        stage_update,
        stage_remove,
        add,
        update,
        remove,
        reset_tst,
        unset_mhash,
    )
