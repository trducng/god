"""Add operation"""
from pathlib import Path

from god.base import read_HEAD, update_HEAD
from god.branches.refs import get_ref, update_ref
from god.branches.track_changes import (
    track_staging_changes,
    track_working_changes,
    track_files,
)
from god.comits.base import read_commit
from god.commits.compare import transform_commit
from god.utils.exceptions import OperationNotPermitted
from god.utils.files import (
    copy_hashed_objects_to_files,
    copy_objects_with_hashes,
    get_files_tst,
    get_objects_tst,
)
from god.index import Index


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
        restore_hashes = [(_[0], _[1]) for _ in index.get_files_info(files=restore)]
        copy_hashed_objects_to_files(restore_hashes, dir_object, base_dir)
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
    move_files=True,
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
    stage_add, stage_update, stage_remove, add, update, remove, _, _ = track_files(
        ["."], index_path, base_dir
    )

    if stage_add or stage_update or stage_remove:
        raise OperationNotPermitted("There are staged files. Aborted")

    # calculate add & remove operations from 2 commits
    add_ops, remove_ops = transform_commit(
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
        move_files=True,
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
        prev = commit_obj["prev"]
        commit2 = prev[0] if isinstance(prev, (list, tuple)) else prev

    # construct index and optionally revert files
    _checkout_between_commits(
        commit1,
        commit2,
        commit_dir,
        commit_dirs_dir,
        index_path,
        obj_dir,
        base_dir,
        move_files=False,
    )

    # update branch ref
    update_ref(refs, commit2, ref_dir)
