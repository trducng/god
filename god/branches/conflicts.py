from collections import Counter
from pathlib import Path

import yaml

from god.comits.base import exists_in_commit
from god.utils.exceptions import FileExisted, OperationNotPermitted
from god.utils.files import (
    copy_hashed_objects_to_files,
)


def get_conflict_dir(commit1, commit2):
    """Get the conflict directory name

    Conflict folder has name `godconflicts.<hash1>-<hash2>`.

    # Args:
        commit1 <str>: the commit id 1 (from)
        commit2 <str>: the commit id 2 (to)

    # Returns:
        <str>: directory name
    """
    conflict_dir = f"godconflicts.{commit1[:6]}-{commit2[:6]}"

    return conflict_dir


def create_conflict_dir(conflicts, conflict_dir, obj_dir, base_dir):
    """Construct conflict directory

    Conflict folder has name `godconflicts.<hash1>-<hash2>` from `get_conflict_dir`,
    that contains:
        - conflicts.yml
        - file objects (copied from object hash folder if `obj_dir` is not None)
        (filename is the hash)

    The newly created `conflicts.yml` has this follow structure:
        ```
        {
            filepath1: {
                type: "++"      # both add
                ours: hasha
                theirs: hashb
            },

            filepath2: {
                type: "+-"      # we add, they delete
                ours: hashc
                theirs: hashd
            },

            filepath3: {
                type: "-+"      # we delete, they add
                ours: hashe
                theirs: hashf
            }
        }
        ```

    The resolved `conflicts.yml` has additionally `decision` and optionally `rename`
    entries:
        ```
        {
            filepath1: {
                type: "++"
                ours: hasha
                theirs: hashb
                decision: ours          # take ours
                move: filepath1a        # keep theirs, but move to filepath1a
            },

            filepath2: {
                type: "+-"
                ours: hashc
                theirs: hashd
                decision: theirs
            },

            filepath3: {
                type: "-+"
                ours: hashe
                theirs: hashf
                decision: ours
            }
        }
        ```

    # Args:
        conflicts <[(str, str, str, str, str)]>: conflict information, include:
            `filepath`, "+"/"-", `hash1`, "+"/"-", `hash2`
        conflict_dir <str>: the name of conflict directory (not full path)
        obj_dir <str>: the path to object directory, can be None
        base_dir <str>: project base directory
    """
    conflict_path = Path(base_dir, conflict_dir)
    if conflict_path.exists():
        raise FileExisted(
            f"Cannot create conflict directory {conflict_path}. Already existed"
        )

    conflicts_content = {
        _[0]: {"type": f"{_[1]}{_[3]}", "ours": _[2], "theirs": _[4]} for _ in conflicts
    }

    conflict_path.mkdir(exist_ok=False)
    with Path(conflict_path, "conflicts.yml").open("w") as f_out:
        yaml.safe_dump(conflicts_content, f_out)

    if obj_dir:
        to_copy = []
        for each_conflict in conflicts:
            to_copy.append((Path(conflict_dir, each_conflict[2]), each_conflict[2]))
            to_copy.append((Path(conflict_dir, each_conflict[4]), each_conflict[4]))
        copy_hashed_objects_to_files(to_copy, obj_dir, base_dir)


def verify_conflict_resolution(
    conflict_resolution,
    conflicts,
    commit1,
    commit2,
    commit_dir,
    commit_dirs_dir,
    add_ops2,
    remove_ops2,
):
    """Verify that conflict resolution is valid

    # Args:
        conflict_resolution <{str: {}}>: conflict resolution entries from conflicts.yml
        conflicts <[]>: conflict entries from comparing commit
        commit1 <str>: the commit id of `ours`
        commit2 <str>: the commit id of `theirs`
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory
        add_ops2 <{str: str}>: the filename and hash to add
        remove_ops2 <{str: str}>: the filename and hash to remove

    # Returns:
        <{str: str}>: the filename and hash to add
        <{str: str}>: the filename and hash to remove
    """
    conflicts_content = {
        _[0]: {"type": f"{_[1]}{_[3]}", "ours": _[2], "theirs": _[4]} for _ in conflicts
    }

    type_errors = []  # document all errors in 1 pass to save time
    hash_errors = []
    filepath_errors = []
    decision_errors = []
    move_errors = []

    possible_decisions = set(["ours", "theirs"])

    # check moves
    all_moves = sorted([_["move"] for _ in conflict_resolution if "move" in _])
    exists1 = exists_in_commit(all_moves, commit1, commit_dir, commit_dirs_dir)
    for move, exist in zip(all_moves, exists1):
        if exist:
            move_errors.append(f"move path {move} exists in our branch")

    exists2 = exists_in_commit(all_moves, commit2, commit_dir, commit_dirs_dir)
    for move, exist in zip(all_moves, exists2):
        if exist:
            move_errors.append(f"move path {move} exists in taret branch")

    counter = Counter(all_moves)
    for move, count in counter.items():
        if count > 1:
            move_errors.append(
                f"move path {move} appears multiple time in conflict resolution"
            )

    # check missing entries
    for filepath in list(
        set(conflicts_content.keys()).difference(conflict_resolution.keys())
    ):
        filepath_errors.append(f"missing file path {filepath}")

    for filepath, reso in conflict_resolution.items():

        has_error = False
        if filepath not in conflicts_content:
            # if there are extra unknown filepaths
            filepath_errors.append(filepath)
            has_error = True

        if "decision" not in reso:
            # if there is no decision
            decision_errors.append(f"missing decision for {filepath}")
            has_error = True

        decision = reso.get("decision", None)
        if decision is not None and decision not in ["ours", "theirs"]:
            decision_errors.append(
                f'unknown decision "{decision}" in {filepath}, '
                f"should be {possible_decisions}"
            )
            has_error = True

        type_ = reso.get("type", "")
        if type_ != conflicts_content[filepath]["type"]:
            # if there is mismatch in 'type'
            type_errors.append(
                f"mismatch type in {filepath}, should be "
                f'"{conflicts_content[filepath]["type"]}" but "{type_}"'
            )
            has_error = True

        if "move" in reso and "-" in type_:
            # 'move' does not work with '+-' and '-+' type
            move_errors.append(
                f"`move` is not applicable for type {type_} in {filepath}, "
                f"please remove"
            )
            has_error = True

        hash_ours = reso.get("ours", "")
        if hash_ours != conflicts_content[filepath]["ours"]:
            hash_errors.append(
                f"unknown from for {filepath}, should be "
                f'{conflicts_content[filepath]["ours"]} but {hash_ours}'
            )
            has_error = True

        hash_theirs = reso.get("theirs", "")
        if hash_theirs != conflicts_content[filepath]["theirs"]:
            hash_errors.append(
                f"unknown from for {filepath}, should be "
                f'{conflicts_content[filepath]["theirs"]} but {hash_theirs}'
            )
            has_error = True

        if has_error:
            continue

        if type_ == "++":
            if reso["decision"] == "ours":
                if "move" in reso:
                    add_ops2[reso["move"]] = add_ops2.pop(filepath)
                    remove_ops2.pop(filepath, None)
                else:
                    add_ops2.pop(filepath, None)
                    remove_ops2.pop(filepath, None)
            else:
                remove_ops2[filepath] = hash_ours
                if "move" in reso:
                    add_ops2[reso["move"]] = hash_ours
            continue

        if type_ == "+-":
            add_ops2.pop(filepath, None)
            if reso["decision"] == "ours":
                remove_ops2.pop(filepath, None)
            else:
                remove_ops2[filepath] = hash_ours
            continue

        if type_ == "-+":
            if reso["decision"] == "ours":
                add_ops2.pop(filepath, None)
                remove_ops2.pop(filepath, None)
            else:
                remove_ops2.pop(filepath, None)

    errors = type_errors + hash_errors + filepath_errors + decision_errors + move_errors
    if errors:
        for _ in errors:
            print(_)
        raise OperationNotPermitted("Conflict not resolved")

    return add_ops2, remove_ops2
