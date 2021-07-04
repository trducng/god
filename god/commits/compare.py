from god.commits.base import get_files_hashes_in_commit
from god.core.files import compare_files_states


def transform_commit(commit1, commit2, commit_dir, commit_dirs_dir):
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
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory

    # Returns
        <{}>: files newly added (recursively)
        <{}>: files newly removed (recursively)
    """
    files_hashes1 = (
        {}
        if commit1 is None
        else get_files_hashes_in_commit(commit1, commit_dir, commit_dirs_dir)
    )
    files_hashes2 = get_files_hashes_in_commit(commit2, commit_dir, commit_dirs_dir)

    return compare_files_states(files_hashes1, files_hashes2)
