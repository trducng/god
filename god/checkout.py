from pathlib import Path
from typing import List

from god.commits.base import read_commit
from god.commits.compare import transform_commit_id
from god.core.files import get_files_tst
from god.core.head import read_HEAD, update_HEAD
from god.core.refs import get_ref, update_ref
from god.index.base import Index
from god.index.trackchanges import track_staging_changes, track_working_changes
from god.index.utils import column_index
from god.plugins.base import installed_plugins, plugin_endpoints
from god.utils.exceptions import OperationNotPermitted


def restore_staged_one(fds: List[str], index_path: str, base_dir: str):
    """Restore files from the staging area to the working area

    This operation will:
        - Delete index entries that are newly added
        - Remove `mhash` in index for entries that are updated
        - Revert `timestamp` to ones in the index
        - Unmark `remove` to NULL for entries that are marked removed

    Args:
        fds: the directory to add (absolute path)
        index_path: path to index file
        base_dir: project base directory
    """
    stage_add, stage_update, stage_remove = track_staging_changes(
        fds, index_path, base_dir
    )

    with Index(index_path) as index:
        index.revert(items=stage_update, mhash=True, remove=False)
        index.revert(items=stage_remove, mhash=False, remove=True)
        index.delete(items=stage_add, staged=False)


def restore_staged(fds: List[str], plugins: List[str]):
    """Restore files from the staging area to the working area

    This operation will:
        - Delete index entries that are newly added
        - Remove `mhash` in index for entries that are updated
        - Revert `timestamp` to ones in the index
        - Unmark `remove` to NULL for entries that are marked removed

    # Args:
        fds: the directory to add (absolute path)
        plugins: list of plugin names that will be restored
    """
    if not fds and not plugins:
        # assume restore all
        fds = ["."]
        plugins = ["files", "configs", "plugins"] + installed_plugins()

    for name in plugins:
        endpoints = plugin_endpoints(name)
        restore_staged_one(
            fds=fds, index_path=endpoints["index"], base_dir=endpoints["tracks"]
        )


def restore_working_one(fds, index_path, base_dir):
    """Revert modified and deleted files from working area to last commit

    This operation only applies to unstaged changes.
    This operation will:
        - In case file is modified, use the version from commit, and modify index
        timestamp
        - In case file is deleted, use the version from commit, and modify index
        timestamp
        - In case file is added, leave it untouched

    Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dir_obj <str>: the path to object directory
        base_dir <str>: project base directory
    """
    _, update, remove, _, _ = track_working_changes(fds, index_path, base_dir)

    restore = [_[0] for _ in update] + remove
    iname = column_index("name")
    ihash = column_index("hash")
    imhash = column_index("mhash")
    with Index(index_path) as index:
        # use latest staged or commited version
        restore_hashes = [
            (str(Path(base_dir, each[iname])), each[imhash] or each[ihash])
            for each in index.get_files(names=restore, get_remove=False, not_in=False)
        ]
        from god.storage.commons import get_backend

        ls = get_backend()
        file_path, hash_value = zip(*restore_hashes)
        ls.get_objects(list(hash_value), list(file_path))
        # tsts = get_files_tst(restore, base_dir)
        # index.update(reset_tst=list(zip(restore, tsts)))


def restore_working(fds: List[str], plugins: List[str]):
    """Revert modified and deleted files from working area to last commit

    This operation only applies to unstaged changes.
    This operation will:
        - In case file is modified, use the version from commit, and modify index
        timestamp
        - In case file is deleted, use the version from commit, and modify index
        timestamp
        - In case file is added, leave it untouched

    Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dir_obj <str>: the path to object directory
        base_dir <str>: project base directory
    """
    if not fds and not plugins:
        # assume restore all
        fds = ["."]
        plugins = ["files", "configs", "plugins"] + installed_plugins()

    for name in plugins:
        endpoints = plugin_endpoints(name)
        restore_working_one(
            fds=fds, index_path=endpoints["index"], base_dir=endpoints["tracks"]
        )


def compare_plugins(commit1, commit2):
    """Compare the state of plugins between commit1, commit2.
    It returns:
        - new_plugs: plugins not in commit1 but in commit2
        - update_plugs: plugins in both commit1 and commit2, but are different values
        - remove_plugs: plugins in commit2 but not in commit1
        - unchanged_plugs: same plugins, same value between commit1 and commit2
    """
    tracks1 = read_commit(commit1)["tracks"] if commit1 else {}
    tracks2 = read_commit(commit2)["tracks"]

    new_plugs = list(sorted(set(tracks2.keys()).difference(tracks1.keys())))
    remove_plugs = list(sorted(set(tracks1.keys()).difference(tracks2.keys())))
    update_plugs = []
    unchanged_plugs = []
    for plug in list(sorted(set(tracks1.keys()).intersection(tracks2.keys()))):
        if tracks1[plug] == tracks2[plug]:
            unchanged_plugs.append(plug)
        else:
            update_plugs.append(plug)

    return (
        new_plugs,
        list(sorted(update_plugs)),
        remove_plugs,
        list(sorted(unchanged_plugs)),
    )


def _checkout_between_commits(
    commit1,
    commit2,
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
        obj_dir <str>: the path to object directory
        move_files <bool>: if true, move files in working directory to match index
    """
    # get staged and unstaged information
    import shutil

    from god.core.status import status
    from god.plugins.base import plugin_endpoints
    from god.utils.process import communicate

    new_plugs, update_plugs, remove_plugs, _ = compare_plugins(commit1, commit2)

    # plugins = ["files", "configs", "plugins"] + installed_plugins()
    statuses = status(["."], update_plugs)
    abort = False
    for plugin_name, (
        stage_add,
        stage_update,
        stage_remove,
        _,
        _,
        _,
        _,
        _,
    ) in statuses.items():
        if stage_add or stage_update or stage_remove:
            print(f'There are staged files for "{plugin_name}"')
            abort = True
    if abort:
        print("Aborted")
        return

    for plugin_name in update_plugs:
        _, _, _, add, update, remove, _, _ = statuses[plugin_name]
        endpoints = plugin_endpoints(plugin_name)

        # calculate add & remove operations from 2 commits
        add_ops, remove_ops = transform_commit_id(commit1, commit2, plugin_name)

        # ignore operations involving unstaged files
        # skips = set([_[0] for _ in add] + [_[0] for _ in update] + remove)
        skips = {}

        # remove files
        for fp in remove_ops.keys():
            if fp in skips:
                continue
            Path(endpoints["tracks"], fp).unlink()

        # add files
        add = [
            (str(Path(endpoints["tracks"], fp)), fh)
            for fp, fh in add_ops.items()
            if fp not in skips
        ]
        if add:
            communicate(command=["god", "storages", "get-objects"], stdin=add)

        # construct index
        add_fps = list(add_ops.keys())
        add_fhs = list(add_ops.values())
        tsts = get_files_tst(add_fps, endpoints["tracks"])
        add = list(zip(add_fps, add_fhs, tsts))

        with Index(endpoints["index"]) as index:
            index.delete(items=list(remove_ops.keys()), staged=False)
            index.add(items=add, staged=False)

    for plugin_name in new_plugs:
        endpoints = plugin_endpoints(plugin_name)
        add_ops, _ = transform_commit_id(None, commit2, plugin_name)
        add_fps = list(add_ops.keys())
        add_fhs = list(add_ops.values())

        add = [(str(Path(endpoints["tracks"], fp)), fh) for fp, fh in add_ops.items()]
        if add:
            communicate(command=["god", "storages", "get-objects"], stdin=add)
        # copy all files
        # create index
        tsts = get_files_tst(add_fps, endpoints["tracks"])
        add = list(zip(add_fps, add_fhs, tsts))

        index = Index(endpoints["index"])
        index.build(force=True)
        with Index(endpoints["index"]) as index:
            index.add(items=add, staged=False)

    for plugin_name in remove_plugs:
        # @PRIORITY2: remove all files
        # remove index
        endpoints = plugin_endpoints(plugin_name)
        Path(endpoints["index"]).unlink()
        shutil.rmtree(endpoints["tracks"])


def checkout(
    ref_dir,
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
        commit1=commit1,
        commit2=commit2,
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
    ref_dir,
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
    refs, _ = read_HEAD(head_file)

    # collect the commit
    commit1 = get_ref(refs, ref_dir)
    commit2 = commit1
    for _ in range(head_past):
        commit_obj = read_commit(commit2)
        prev = commit_obj["prev"]
        commit2 = prev[0] if isinstance(prev, (list, tuple)) else prev

    # construct index and optionally revert files
    _checkout_between_commits(
        commit1=commit1,
        commit2=commit2,
    )

    # update branch ref
    update_ref(refs, commit2, ref_dir)
