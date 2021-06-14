"""Branch operations"""


def checkout(commit1, commit2, commit_dir, commit_dirs_dir):
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
        - Update HEAD

    # Args
        commit1 <str>: the hash of commit 1
        commit2 <str>: the hash of commit 2. If None, this is the first time.
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory
    """
    pass

