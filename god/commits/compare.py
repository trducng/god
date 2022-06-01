from typing import Dict, Tuple, Union

from god.commits.base import get_files_hashes_in_commit_dir, read_commit
from god.core.files import compare_files_states


def transform_commit_obj(
    commit_obj1: Union[Dict, None], commit_obj2: Dict, plugin: str = "files"
) -> Tuple[Dict, Dict]:
    """Get add and remove operations to transform from state1 to state2

    The files from state1 to state2 are as follow:
        - Same path - Same hash -> Ignore
        - Same path - Different hash -> commit2 is added, commit1 is removed
        - Commit1 doesn't have - commit2 has -> Files are added
        - Commit1 has - Commit2 doesn't have -> Files are moved

    The output of this function serves:
        - file_add: add these files to the table in the new commit
        - file_remove: remove these files from the table in the new commit

    # Args
        commit1 <str>: the hash of commit 1. If None, this is the first time.
        commit2 <str>: the hash of commit 2.

    # Returns
        <{fn: fh}>: files newly added (recursively)
        <{fn: fh}>: files newly removed (recursively)
    """
    files_hashes1 = (
        {}
        if commit_obj1 is None
        else get_files_hashes_in_commit_dir(commit_obj1["tracks"][plugin], prefix=".")
    )
    files_hashes2 = get_files_hashes_in_commit_dir(
        commit_obj2["tracks"][plugin], prefix="."
    )

    return compare_files_states(files_hashes1, files_hashes2)


def transform_commit_id(
    commit_id1: Union[str, None], commit_id2: str, plugin: str = "files"
) -> Tuple[Dict, Dict]:
    """Get add and remove operations to transform from commit_id1 to commit_id2"""
    commit_obj1: Union[None, Dict] = None
    if isinstance(commit_id1, str):
        commit_obj1 = read_commit(commit_id1)

    commit_obj2 = read_commit(commit_id2)

    return transform_commit_obj(commit_obj1, commit_obj2, plugin)
