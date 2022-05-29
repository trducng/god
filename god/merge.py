"""
Diff requirements:
    - The diff should be able to show that there is a conflict. So that during merge:
        + if there is a conflict, we will let user knows to resolve.
        + if there isn't a conflict, we proceed with automatically building the
        new complete entry.
    - `god` does not know the optimal way to show diff for (1) a text file or (2) a
    plugin.
    - `god` does not know the optimal way to merge generally, because the diff
    instruction can come from a single or multiple files
    - `god` should be flexible to show diff for multiple file types -> Seems important
    to store the diff locally, and then having each content rendered

If there is a conflict, `god` will guide the user through the process of handling that
conflict.

If there is no conflict, `god` will have to be sure that it is *exactly* no conflict,
and `god` will work with each respective plugin to handle that.

Standardized format representing longest common blocks for 2 files (JSON format):
    [
        [(start1a, stop1a), (start1b, stop1b)],
        [(start2a, stop2a), (start2b, stop2b)],
        [(start3a, stop3a), (start3b, stop3b)],
        ...
    ]

`god` shows diff on the console. `god` will by default treat every file as binary. It
will relies on the plugin to show semantically meaningful diff. Also, it relies on the
plugins to unfold the appropriate merge strategy. Otherwise, it will resort back to
the default code conflict handler.

@PRIORITY0: remove the "Seems important...", and put this rationale into technical
document.
"""
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

from god.branches.conflicts import (
    create_conflict_dir,
    get_conflict_dir,
    verify_conflict_resolution,
)
from god.commit import commit
from god.commits.base import get_latest_parent_commit, read_commit
from god.commits.compare import transform_commit
from god.core.files import get_files_tst
from god.core.index import Index
from god.core.refs import get_ref, update_ref
from god.utils.process import communicate, delegate


def diff(commit1: str, commit2: str, plugins: List[str]):
    """Show the diff between commit1 and commit2"""
    for plugin in plugins:
        add, remove = transform_commit(commit1, commit2, plugin)
        update_fn = set(add.keys()).intersection(remove.keys())
        update = {}
        for each in update_fn:
            update[each] = [remove.pop(each), add.pop(each)]
        delegate(
            ["god", plugin, "hook", "diff"],
            stdin={"add": add, "update": update, "remove": remove},
        )


def check_merge_plugins(
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


def merge(
    branch1,
    branch2,
    ref_dir,
    base_dir,
    user,
    email,
    include,
    exclude,
):
    """Pull changes from `branch2` to `branch1`

    # Args:
        branch1 <str>: the name of source branch
        branch2 <str>: the name of target branch to pull from
        ref_dir <str>: the path to refs directory
        base_dir <str>: the repository directory
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
    plugins, inconsistence = check_merge_plugins(
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

    has_conflict = False
    for plugin in plugins:
        # get operations
        add_ops1, remove_ops1 = transform_commit(
            parent_commit, commit1, plugin  # @PRIORITY0
        )
        add_ops2, remove_ops2 = transform_commit(
            parent_commit, commit2, plugin  # @PRIORITY0
        )

        # check for conflicts
        fp_add_ops1, fp_remove_ops1 = set(add_ops1.keys()), set(remove_ops1.keys())
        fp_add_ops2, fp_remove_ops2 = set(add_ops2.keys()), set(remove_ops2.keys())
        conflicts = []

        for fp in list(fp_add_ops1.intersection(fp_add_ops2)):
            # both commit add/edit the same file
            conflicts.append((fp, "+", add_ops1[fp], "+", add_ops2[fp]))
        for fp in list(
            fp_add_ops1.intersection(fp_remove_ops2.difference(fp_add_ops2))
        ):
            # our commit adds, while the other removes
            conflicts.append((fp, "+", add_ops1[fp], "-", remove_ops2[fp]))
        for fp in list(
            fp_add_ops2.intersection(fp_remove_ops1.difference(fp_add_ops1))
        ):
            # our commit removes, while the other adds
            conflicts.append((fp, "-", remove_ops1[fp], "+", add_ops2[fp]))

        # handle conflict resolution, show this inside the `index` file
        if conflicts:
            conflict_dir = get_conflict_dir(commit1, commit2)
            conflict_path = Path(base_dir, conflict_dir)

            if not conflict_path.exists():
                # if there are no conflict reso folder
                create_conflict_dir(conflicts, conflict_dir, obj_dir, base_dir)
                print(f"Conflict data is saved in {conflict_path}. Please resolve.")
                print("Run `god merge <branch>` again after resolution to continue.")
                return

            # read conflict reso folder
            with (conflict_path / "conflicts.yml").open("r") as f_in:
                conflict_resolution = yaml.safe_load(f_in)

            # verify if it satisfy
            add_ops2, remove_ops2 = verify_conflict_resolution(
                conflict_resolution,
                conflicts,
                commit1,
                commit2,
                commit_dir,
                commit_dirs_dir,
                add_ops2,
                remove_ops2,
            )
    if has_conflict:
        raise RuntimeError("Conflict, please resolve")

    # without conflict, apply the change of `branch2` to `branch1`
    # remove files
    for fp in remove_ops2.keys():
        # if fp in skips:
        #     continue
        Path(base_dir, fp).unlink()

    # add files
    # add = [(fp, fh) for fp, fh in add_ops2.items() if fp not in skips]
    add = [(str(Path(base_dir, fp)), fh) for fp, fh in add_ops2.items()]
    if add:
        communicate(command=["god", "storages", "get-objects"], stdin=add)

    import pdb

    pdb.set_trace()
    # construct index
    add_fps = list(add_ops2.keys())
    add_fhs = list(add_ops2.values())
    tsts = get_files_tst(add_fps, base_dir)

    with Index(index_path) as index:
        index.update(
            new_entries=list(zip(add_fps, add_fhs, tsts)),
            delete=list(remove_ops2.keys()),
        )

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
