"""Add operation"""
from collections import defaultdict
from pathlib import Path
import shutil
import uuid

from god.base import read_HEAD, update_HEAD
from god.commit import get_transform_operations, read_commit
from god.exceptions import OperationNotPermitted
from god.files import (
    get_file_hash,
    copy_objects_with_hashes,
    copy_hashed_objects_to_files,
    get_files_tst,
    get_objects_tst,
    separate_paths_to_files_dirs,
    retrieve_files_info,
    filter_common_parents,
    resolve_paths,
)
from god.index import Index
from god.refs import get_ref, update_ref




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

    files, dirs, unknowns = separate_paths_to_files_dirs(fds, base_dir)
    files_dirs = retrieve_files_info(files, dirs, base_dir)

    index_files_dirs, index_unknowns = defaultdict(list), []
    add, update, remove = [], [], []

    with Index(index_path) as index:
        for fd in fds:
            result = index.match(fd)
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


def track_working_changes(fds, index_path, base_dir, get_remove=True):
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
        fds <str>: the directory to add (absolute path)
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
    if not isinstance(fds, (list, int)):
        fds = [fds]

    fds = resolve_paths(fds, base_dir)
    fds = filter_common_parents(fds)  # list of relative paths to `base_dir`

    files, dirs, unknowns = separate_paths_to_files_dirs(fds, base_dir)
    files_dirs = retrieve_files_info(files, dirs, base_dir)

    index_files_dirs, index_unknowns = defaultdict(list), []
    with Index(index_path) as index:
        for fd in fds:
            result = index.match(fd, get_remove=False)
            if not result:  # not exist
                index_unknowns.append(fd)
                continue
            if result[0][0] == fd:  # single file
                index_files_dirs[str(Path(fd).parent)].append(result[0])
                continue
            for _ in result:  # directory of files
                index_files_dirs[str(Path(_[0]).parent)].append(
                    (Path(_[0]).name, _[1], _[2], _[3], _[4], _[5], _[6], _[7])
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
                if path_files[fn] == index_files[fn][6]:
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


def add(fds, index_path, dir_obj, base_dir):
    """Add the files and directories to staging area

    # Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dr_obj <str>: the path to object directory
        base_dir <str>: project base directory
    """
    add, update, remove, reset_tst, unset_mhash = track_working_changes(
        fds, index_path, base_dir
    )

    # copy files to objects directory
    copy_objects_with_hashes([(each[0], each[1]) for each in add], dir_obj, base_dir)
    copy_objects_with_hashes([(each[0], each[1]) for each in update], dir_obj, base_dir)

    with Index(index_path) as index:
        index.update(
            add=add,
            update=update,
            remove=remove,
            reset_tst=reset_tst,
            unset_mhash=unset_mhash,
        )


def status(fds, index_path, base_dir):
    """Track statuses of the directories

    # Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        base_dir <str>: project base directory
    """
    add, update, remove, reset_tst, unset_mhash = track_working_changes(
        fds, index_path, base_dir, get_remove=False
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


def restore_staged(fds, index_path, dir_obj, base_dir):
    """Restore files from the staging area to the working area

    This operation will:
        - Delete index entries that are newly added
        - Remove `mhash` in index for entries that are updated
        - Revert `timestamp` to ones in the index
        - Unmark `remove` to NULL for entries that are marked removed

    # Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dir_obj <str>: the path to object directory
        base_dir <str>: project base directory
    """
    stage_add, stage_update, stage_remove = track_staging_changes(
        fds, index_path, base_dir
    )

    with Index(index_path) as index:

        # get original time of files in staging areas
        stage_hashes = [_[1] for _ in index.get_files_info(files=stage_update)]
        tsts = get_objects_tst(stage_hashes, dir_obj)
        reset_tst = list(zip(stage_update, tsts))

        index.update(
            reset_tst=reset_tst,
            unset_mhash=stage_update,
            unset_remove=stage_remove,
            delete=stage_add,
        )


def restore_working(fds, index_path, dir_object, base_dir):
    """Revert modified and deleted files from working area to last commit

    This operation only applies to unstaged changes.
    This operation will:
        - In case file is modified, use the version from commit, and modify index
        timestamp
        - In case file is deleted, use the version from commit, and modify index
        timestamp
        - In case file is added, leave it untouched

    # Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dir_obj <str>: the path to object directory
        base_dir <str>: project base directory
    """
    _, update, remove, _, _ = track_working_changes(
        fds, index_path, base_dir, get_remove=False
    )

    restore = [_[0] for _ in update] + remove
    with Index(index_path) as index:
        restore_hashes = [_[1] for _ in index.get_files_info(files=restore)]
        copy_hashed_objects_to_files(
            list(zip(restore, restore_hashes)), dir_object, base_dir
        )
        tsts = get_files_tst(restore, base_dir)
        index.update(reset_tst=list(zip(restore, tsts)))


def _checkout_between_commits(
    commit1,
    commit2,
    commit_dir,
    commit_dirs_dir,
    index_path,
    obj_dir,
    base_dir,
    move_files=True
):
    """Perform checkout from commit1 to commit2

    This operation checkout the data from commit1 to commit2. The commit1 should be
    the current commit that the repo is in. Specifically, this operation:
        - Check if there is any staged files, if yes, abort.
        - Check if there is any unstaged files, if yes, these files will be ignored
        when checking out to commit2
        - Calculate add/remove operations from commit1 to commit2
        - Ignore operations involving unstaged files
        - Simplify any possible add/remove operations into move operation for quickly
        moving files
        - For remaining items, copy from hashed `objects`
        - Construct commit index

    # Args
        commit1 <str>: the commit id 1 (from)
        commit2 <str>: the commit id 2 (to)
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory
        index_path <str>: path to index file
        obj_dir <str>: the path to object directory
        base_dir <str>: project base directory
        move_files <bool>: if true, move files in working directory to match index
    """
    # get staged and unstaged information
    stage_add, stage_update, stage_remove, add, update, remove, _, _ = status(
        ["."], index_path, base_dir
    )

    if stage_add or stage_update or stage_remove:
        raise OperationNotPermitted("There are staged files. Aborted")

    # calculate add & remove operations from 2 commits
    add_ops, remove_ops = get_transform_operations(
        commit1, commit2, commit_dir, commit_dirs_dir
    )

    # ignore operations involving unstaged files
    skips = set([_[0] for _ in add] + [_[0] for _ in update] + remove)

    if move_files:
        # remove files
        for fp in remove_ops.keys():
            if fp in skips:
                continue
            Path(base_dir, fp).unlink()

        # add files
        add = [(fp, fh) for fp, fh in add_ops.items() if fp not in skips]
        copy_hashed_objects_to_files(add, obj_dir, base_dir)

    # construct index
    add_fps = list(add_ops.keys())
    add_fhs = list(add_ops.values())
    if move_files:
        tsts = get_files_tst(add_fps, base_dir)
    else:
        tsts = get_objects_tst(add_fhs, obj_dir)

    with Index(index_path) as index:
        index.update(
            new_entries=list(zip(add_fps, add_fhs, tsts)),
            delete=list(remove_ops.keys()),
        )


def checkout(
    commit_dir,
    commit_dirs_dir,
    index_path,
    obj_dir,
    ref_dir,
    base_dir,
    head_file,
    commit1=None,
    commit2=None,
    branch1=None,
    branch2=None,
):
    """Perform checkout from commit1/branch1 to commit2/branch2

    # Args
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory
        index_path <str>: path to index file
        obj_dir <str>: the path to object directory
        ref_dir <str>: the path to refs directory
        base_dir <str>: project base directory
        head_file <str>: path to HEAD file
        commit1 <str>: the commit id 1 (from)
        commit2 <str>: the commit id 2 (to)
        branch1 <str>: the branch name
        branch2 <str>: the branch name
    """
    if commit1 is None and branch1 is None:
        raise OperationNotPermitted("Either commit1 or branch1 must be specified")

    if commit2 is None and branch2 is None:
        raise OperationNotPermitted("Either commit2 or branch2 must be specified")

    if branch1:
        commit1 = get_ref(branch1, ref_dir)

    if branch2:
        commit2 = get_ref(branch2, ref_dir)

    _checkout_between_commits(
        commit1,
        commit2,
        commit_dir,
        commit_dirs_dir,
        index_path,
        obj_dir,
        base_dir,
        move_files=True
    )

    if branch2:
        update_HEAD(head_file, REFS=branch2, COMMITS=None)
    else:
        update_HEAD(head_file, REFS=None, COMMITS=commit2)


def checkout_new_branch(branch, commit_id, ref_dir, head_file):
    """Create a new branch from a current commit and register

    # Args
        branch <str>: the branch that contains commit 2 (to)
        commit_id <str>: path to HEAD file
        ref_dir <str>: the path to refs directory
        head_file <str>: path to HEAD file
    """
    if get_ref(branch, ref_dir):
        raise OperationNotPermitted(f"Branch '{branch}' already exists")

    update_ref(branch, commit_id, ref_dir)
    update_HEAD(head_file, REFS=branch, COMMITS=None)


def reset(
    head_past,
    hard,
    commit_dir,
    commit_dirs_dir,
    index_path,
    obj_dir,
    ref_dir,
    base_dir,
    head_file,
):
    """Reset branch to previous commit

    # Args
        head_past <int>: the upper history to reset to
        hard <bool> if true, complete convert to history, else changed files to unstaged
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory
        index_path <str>: path to index file
        obj_dir <str>: the path to object directory
        ref_dir <str>: the path to refs directory
        base_dir <str>: project base directory
        head_file <str>: path to HEAD file
    """
    refs, _, _ = read_HEAD(head_file)

    # collect the commit
    commit1 = get_ref(refs, ref_dir)
    commit2 = commit1
    for idx in range(head_past):
        commit_obj = read_commit(commit2, commit_dir)
        commit2 = commit_obj["prev"]

    # construct index and optionally revert files
    _checkout_between_commits(
        commit1,
        commit2,
        commit_dir,
        commit_dirs_dir,
        index_path,
        obj_dir,
        base_dir,
        move_files=False
    )

    # update branch ref
    update_ref(refs, commit2, ref_dir)
