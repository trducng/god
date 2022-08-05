"""Show diff"""
import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

from god.commit import commit
from god.commits.base import get_latest_parent_commit, read_commit
from god.commits.compare import transform_commit_id, transform_commit_obj
from god.core.files import get_file_hash, get_files_tst, is_binary
from god.core.refs import get_ref, update_ref
from god.index.base import Index
from god.index.utils import column_index
from god.plugins.base import load_manifest, plugin_endpoints
from god.utils.merge_text import Merge3
from god.utils.process import communicate, delegate


def diff(commit1: str, commit2: str, plugins: List[str]):
    """Show the diff between commit1 and commit2"""
    for plugin in plugins:
        add, remove = transform_commit_id(commit1, commit2, plugin)
        update_fn = set(add.keys()).intersection(remove.keys())
        update = {}
        for each in update_fn:
            update[each] = [remove.pop(each), add.pop(each)]
        delegate(
            ["god", plugin, "hook", "diff"],
            stdin={"add": add, "update": update, "remove": remove},
        )


def are_plugins_valid_for_merge(
    commit_obj1: Dict,  # our commit object
    commit_obj2: Dict,  # their commit object
    commit_obj_parent: Dict,  # shared parrent commit object
) -> Tuple[List[str], List[str]]:
    """Check for inconsistent plugins in a 3-way merge

    A plugin becomes inconsistent when it is removed in 1 branch, and updated in
    another. When a plugin is inconsistent, it's unclear whether the resulting merged
    commit should contain that plugin or not.

    Args:
        commit_obj1: our commit object
        commit_obj2: their commit object
        commot_obj_parent: shared parrent commit object

    Returns:
        List[str]: shared valid plugins
        List[str]: inconsistent plugins
    """
    plugins1 = set(commit_obj1["tracks"].keys())
    plugins2 = set(commit_obj2["tracks"].keys())
    parent_plugins = set(commit_obj_parent["tracks"].keys())

    removed_plugins1 = parent_plugins.difference(plugins1)
    removed_plugins2 = parent_plugins.difference(plugins2)

    updated_plugins1 = [
        each
        for each in list(parent_plugins.intersection(plugins1))
        if commit_obj1["tracks"][each] != commit_obj_parent["tracks"][each]
    ]
    updated_plugins2 = [
        each
        for each in list(parent_plugins.intersection(plugins2))
        if commit_obj1["tracks"][each] != commit_obj_parent["tracks"][each]
    ]

    inconsistence = removed_plugins1.intersection(updated_plugins2)
    inconsistence.update(removed_plugins2.intersection(updated_plugins1))

    return list(plugins1.union(plugins2).difference(inconsistence)), list(inconsistence)


def get_conflicts(index_path: str) -> List[str]:
    """Get conflict files from index

    Args:
        index_path: path to index file

    Returns:
        List[str]: filepath of conflicted files
    """
    with Index(index_path) as index:
        col_name, col_mhash = column_index("name"), column_index("mhash")
        current_conflicts = [
            each[col_name]
            for each in index.get_folder(names=["."], get_remove=False, conflict=True)
            if not each[col_mhash]
        ]
        if current_conflicts:
            return current_conflicts
    return []


def handle_conflicts(index_path, track_dir, temp_dir):
    """Attempt to automatically resolve conflict files"""
    with Index(index_path) as index:
        unresolved_add_add = index.get_conflict_add_add(case=2)

    for name, _, _, _, _, _, _, ctheirs, cbase in unresolved_add_add:
        # retrieve base, ours and theirs
        fd_theirs, theirs = tempfile.mkstemp(dir=temp_dir)
        fd_base, base = tempfile.mkstemp(dir=temp_dir)
        communicate(
            command=["god", "storages", "get-objects"],
            stdin=[[theirs, ctheirs], [base, cbase]],
        )
        os.close(fd_theirs)
        os.close(fd_base)

        # check if any of the 3 files are binary
        binary = False
        for path in (theirs, base, str(Path(track_dir, name))):
            if is_binary(path):
                binary = True

        # if yes, copy to current working directory and continue next iteration
        if binary:
            shutil.copy(theirs, Path(track_dir, f"{name}.theirs.godconfig"))
            shutil.copy(base, Path(track_dir, f"{name}.base.godconfig"))
            continue

        # if no, attempt to fix the text file
        line_base, line_ours, line_theirs = [], [], []
        with open(base) as fi:
            line_base = fi.read().splitlines()
        with Path(track_dir, name).open("r") as fi:
            line_ours = fi.read().splitlines()
        with open(theirs) as fi:
            line_theirs = fi.read().splitlines()

        merge = Merge3(base=line_base, a=line_ours, b=line_theirs)
        merged, conflict = merge.merge_lines(
            name_a="ours", name_b="theirs", name_base="base"
        )

        with Path(track_dir, name).open("w") as fo:
            fo.writelines(merged)
        tsts = get_files_tst([name], track_dir)[0]
        hash = get_file_hash(Path(track_dir, name))

        if not conflict:
            # if fixing the text file ok, resolve in the index
            with Index(index_path) as index:
                index.add(items=[(name, hash, tsts)], staged=True)


def merge_plugin(
    our_commit: Dict,
    their_commit: Dict,
    parent_commit: Dict,
    plugin: str,
    index_path: str,
    track_dir: str,
    hooks: Dict,
) -> List[str]:
    """Perform a three-way merge for a plugin

    Returns:
        List[str]: list of conflicted files
    """
    # check for unresolved conflict entries
    current_conflicts: List[str] = get_conflicts(index_path)
    if current_conflicts:
        return current_conflicts

    # get operations
    add_ops1, remove_ops1 = transform_commit_obj(parent_commit, our_commit, plugin)
    add_ops2, remove_ops2 = transform_commit_obj(parent_commit, their_commit, plugin)

    # check for conflicts
    fp_add_ops1, fp_remove_ops1 = set(add_ops1.keys()), set(remove_ops1.keys())
    fp_add_ops2, fp_remove_ops2 = set(add_ops2.keys()), set(remove_ops2.keys())

    # update the working directory and index for valid files in the plugin
    valid_remove = list(fp_remove_ops2.difference(fp_add_ops2).difference(fp_add_ops1))
    for fp in valid_remove:
        Path(track_dir, fp).unlink()
    if valid_remove:
        with Index(index_path) as index:
            index.delete(items=valid_remove, staged=True)

    valid_add = [
        (fp, add_ops2[fp])
        for fp in list(
            fp_add_ops2.difference(fp_remove_ops2).difference(
                fp_remove_ops1.difference(fp_add_ops1)
            )
        )
    ]
    if valid_add:
        communicate(command=["god", "storages", "get-objects"], stdin=valid_add)
        tsts = get_files_tst([_[0] for _ in valid_add], track_dir)
        with Index(index_path) as index:
            index.add(
                items=[
                    (valid_add[_][0], valid_add[_][1], tsts[_])
                    for _ in range(len(tsts))
                ],
                staged=True,
            )

    valid_update = [
        (fp, add_ops2[fp])
        for fp in list(
            fp_add_ops2.intersection(fp_remove_ops2)  # updated in #2
            .difference(fp_remove_ops1)
            .difference(fp_add_ops1)  # untouched in #1
        )
    ]
    if valid_update:
        communicate(command=["god", "storages", "get-objects"], stdin=valid_update)
        tsts = get_files_tst([_[0] for _ in valid_update], track_dir)
        with Index(index_path) as index:
            index.update(
                items=[
                    (valid_update[_][0], valid_update[_][1], tsts[_])
                    for _ in range(len(tsts))
                ]
            )

    # look for conflicts
    conflicts = {}
    for fp in list(fp_add_ops1.intersection(fp_add_ops2)):
        # both commit add/edit the same file
        conflicts[fp] = [add_ops2[fp], remove_ops2.get(fp, "")]
    for fp in list(fp_add_ops1.intersection(fp_remove_ops2.difference(fp_add_ops2))):
        # our commit adds, while the other removes
        conflicts[fp] = ["", remove_ops1.get(fp, "")]
    for fp in list(fp_add_ops2.intersection(fp_remove_ops1.difference(fp_add_ops1))):
        # our commit removes, while the other adds
        conflicts[fp] = [add_ops2[fp], remove_ops2[fp]]

    # handle conflict resolution, show this inside the `index` file
    if conflicts:
        # update the conflict information to the index
        with Index(index_path) as index:
            index.conflict(items=conflicts)

        # send the conflict information to respective plugin
        merge_conflict = hooks.get("merge-conflict", [])
        if merge_conflict:
            communicate(command=merge_conflict)
        else:
            handle_conflicts(
                index_path=index_path,
                track_dir=track_dir,
                temp_dir="/tmp",  # @PRIORITY2: build the cache system
            )
        current_conflicts = get_conflicts(index_path)

    return current_conflicts


def merge(
    branch1,
    branch2,
    ref_dir,
    user,
    email,
    include,
    exclude,
) -> str:
    """Pull changes from `branch2` to `branch1`

    # Args:
        branch1 <str>: the name of source branch
        branch2 <str>: the name of target branch to pull from
        ref_dir <str>: the path to refs directory
        user <str>: the commiter username
        email <str>: the committer email
    """
    # get 3-way commit information
    commit1 = get_ref(branch1, ref_dir)
    commit2 = get_ref(branch2, ref_dir)
    parent_commit = get_latest_parent_commit(commit1, commit2)
    if not parent_commit:
        raise RuntimeError(
            f'No shared history between branches "{branch1}" and "{branch2}"'
        )
    commit_obj1, commit_obj2 = read_commit(commit1), read_commit(commit2)
    commit_obj_parent = read_commit(parent_commit)

    # collect plugins
    plugins, inconsistence = are_plugins_valid_for_merge(
        commit_obj1, commit_obj2, commit_obj_parent
    )
    inconsistence = set(inconsistence).difference(include).difference(exclude)
    if inconsistence:
        raise RuntimeError(
            f"Inconsistent used plugins: {inconsistence}. "
            "Please use --include and --exclude to specify plugins to use in the "
            "merged commit."
        )
    plugins = list(set(plugins).union(include))

    # run merge for each plugin
    has_conflicts = False
    for plugin in plugins:
        endpoints = plugin_endpoints(plugin)
        if merge_plugin(
            our_commit=commit_obj1,
            their_commit=commit_obj2,
            parent_commit=commit_obj_parent,
            plugin=plugin,
            index_path=endpoints["index"],
            track_dir=endpoints["tracks"],
            hooks=load_manifest(plugin).get("commands", {}).get("merge", {}),
        ):
            has_conflicts = True
    if has_conflicts:
        raise RuntimeError("Has conflicts")

    current_commit = commit(
        user=user,
        email=email,
        message=f"Merge from {branch2} to {branch1}",
        prev_commit=[commit1, commit2],
    )

    update_ref(branch1, current_commit, ref_dir)
    return current_commit


def merge_continue(
    branch1,
    branch2,
    ref_dir,
    user,
    email,
    include,
    exclude,
) -> str:
    """Pull changes from `branch2` to `branch1`

    # Args:
        branch1 <str>: the name of source branch
        branch2 <str>: the name of target branch to pull from
        ref_dir <str>: the path to refs directory
        user <str>: the commiter username
        email <str>: the committer email
    """
    # get 3-way commit information
    commit1 = get_ref(branch1, ref_dir)
    commit2 = get_ref(branch2, ref_dir)
    parent_commit = get_latest_parent_commit(commit1, commit2)
    if not parent_commit:
        raise RuntimeError(
            f'No shared history between branches "{branch1}" and "{branch2}"'
        )
    commit_obj1, commit_obj2 = read_commit(commit1), read_commit(commit2)
    commit_obj_parent = read_commit(parent_commit)

    # collect plugins
    plugins, inconsistence = are_plugins_valid_for_merge(
        commit_obj1, commit_obj2, commit_obj_parent
    )
    inconsistence = set(inconsistence).difference(include).difference(exclude)
    if inconsistence:
        raise RuntimeError(
            f"Inconsistent used plugins: {inconsistence}. "
            "Please use --include and --exclude to specify plugins to use in the "
            "merged commit."
        )
    plugins = list(set(plugins).union(include))

    # run merge for each plugin
    has_conflicts = False
    for plugin in plugins:
        endpoints = plugin_endpoints(plugin)
        if get_conflicts(endpoints["index"]):
            has_conflicts = True

    if has_conflicts:
        raise RuntimeError("Has conflicts")

    current_commit = commit(
        user=user,
        email=email,
        message=f"Merge from {branch2} to {branch1}",
        prev_commit=[commit1, commit2],
    )

    update_ref(branch1, current_commit, ref_dir)
    return current_commit


if __name__ == "__main__":
    # diff(
    #     commit1="d83d11f2e46e8ae88ab6c81b3f8c6e271a72bca46d81440c2d77b983be47fecb",
    #     commit2="18bd5e9b3b096cc0aee69acadc8963f794f54db0da2a991379ef566c60f3eed9",
    #     plugins=["records", "files"]
    # )

    diff(
        commit1="77b95c6ca0e02bb0b01812dbaca9d4ac124ecb9df8d5cbcfb8336a663d8b82a1",
        commit2="18bd5e9b3b096cc0aee69acadc8963f794f54db0da2a991379ef566c60f3eed9",
        plugins=["records"],
    )
