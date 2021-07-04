import shutil
from pathlib import Path

import yaml

from god.branches.conflicts import verify_conflict_resolution, create_conflict_dir, get_conflict_dir
from god.branches.refs import get_ref, update_ref
from god.commit import commit
from god.comits.base import get_latest_parent_commit
from god.commits.compare import transform_commit
from god.utils.files import copy_hashed_objects_to_files, get_files_tst
from god.index import Index


def merge(
    branch1,
    branch2,
    ref_dir,
    commit_dir,
    commit_dirs_dir,
    index_path,
    obj_dir,
    base_dir,
    user,
    email,
):
    """Pull changes from `branch2` to `branch1`

    # Args:
        branch1 <str>: the name of source branch
        branch2 <str>: the name of target branch to pull from
        ref_dir <str>: the path to refs directory
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory
        index_path <str>: the path to index file
        obj_dir <str>: the path to directory containing object hashes
        base_dir <str>: the repository directory
        user <str>: the commiter username
        email <str>: the committer email
    """
    # get commit information
    commit1 = get_ref(branch1, ref_dir)
    commit2 = get_ref(branch2, ref_dir)
    parent_commit = get_latest_parent_commit(commit1, commit2, commit_dir)

    # get operations
    add_ops1, remove_ops1 = transform_commit(
        parent_commit, commit1, commit_dir, commit_dirs_dir
    )
    add_ops2, remove_ops2 = transform_commit(
        parent_commit, commit2, commit_dir, commit_dirs_dir
    )

    # check for conflicts
    fp_add_ops1, fp_remove_ops1 = set(add_ops1.keys()), set(remove_ops1.keys())
    fp_add_ops2, fp_remove_ops2 = set(add_ops2.keys()), set(remove_ops2.keys())
    conflicts = []

    for fp in list(fp_add_ops1.intersection(fp_add_ops2)):
        # both commit add/edit the same file
        conflicts.append((fp, "+", add_ops1[fp], "+", add_ops2[fp]))
    for fp in list(fp_add_ops1.intersection(fp_remove_ops2.difference(fp_add_ops2))):
        # our commit adds, while the other removes
        conflicts.append((fp, "+", add_ops1[fp], "-", remove_ops2[fp]))
    for fp in list(fp_add_ops2.intersection(fp_remove_ops1.difference(fp_add_ops1))):
        # our commit removes, while the other adds
        conflicts.append((fp, "-", remove_ops1[fp], "+", add_ops2[fp]))

    # handle conflict resolution
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

        # Possible representation: construct a stand-alone, reserved folder
        # Each conflict is represented as a text file (e.g. 1 - fn)
        # . orihash - name
        # +/- our hash (or empty if -)
        # name1 (if change name)
        # +/- their hash (or empty if -)
        # name2 (if change name)

        # The conflict resolution should be written in an extensible manner
        # because of the large amount of filetypes, and you want to present
        # these conflicts in a way that users can tell the difference.
        # and then, they can pick the ones on the left, or on the right, or both, and
        # if both, how to change the filename appropriately.

        # Conflict resolution is an independent process. When there is a conflict,
        # layout the `.god` in a way that independent script can support resolution.
        # After that, when the conflict is resolved, the `commit` can pick up
        # and finish

        # Also, the conflict resolution should be extensible because there can be
        # conflicted text file, which is natural to support vim-like conflict
        # resolution mechanism

        # The solution should be file-type independent, there can be numpy, npz...
        # conflict files

        # Autocomplete, local WebUI, or interactive terminal is a good option for this

        # Good source:
        # https://docs.github.com/en/github/managing-files-in-a-repository/working-with-non-code-files
        # https://github.com/ewanmellor/git-diff-image/blob/master/diff-image
        # think about it, vimdiff is just something that takes a git conflict
        # output, render it, and organize by itself in a way that allow for conflict
        # resolution
        # Sound: ffplay -nodisp harry.mp3
        # Video: DISPLAY= mplayer -quiet -vo caca pirates.mp4
        # Over ssh: ssh x@y cat file.mp4 | mplayer -quiet -vo caca -
        # https://www.systutorials.com/mplayer-over-ssh-to-play-movie-from-remote-host/

    # without conflict, apply the change of `branch2` to `branch1`
    # remove files
    for fp in remove_ops2.keys():
        # if fp in skips:
        #     continue
        Path(base_dir, fp).unlink()

    # add files
    # add = [(fp, fh) for fp, fh in add_ops2.items() if fp not in skips]
    add = [(fp, fh) for fp, fh in add_ops2.items()]
    copy_hashed_objects_to_files(add, obj_dir, base_dir)

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
        index_path=index_path,
        commit_dir=commit_dir,
        commit_dirs_dir=commit_dirs_dir,
    )

    update_ref(branch1, current_commit, ref_dir)

    if conflicts:
        shutil.rmtree(conflict_path)
