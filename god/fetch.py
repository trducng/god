import os
from pathlib import Path
from typing import Union

import yaml  # @PRIORITY3: whether to replace yaml with JSON

from god.core.refs import get_ref, is_ref
from god.storage.commons import get_backend


def fetch_object_storage(
    branch: str, ref_remotes_dir: Union[Path, str], remote_path: str, local_path: str
) -> bool:
    """Fetch the remote branch from central repository to local remote

    Args:
        branch: the name of the remote branch to fetch
        ref_remotes_dir: the local directory that store remote ref

    Returns:
        True if remote is different than local, False otherwise
    """
    remote_storage = get_backend(remote_path)

    # check if the object storage is a central remote storage
    if "have_refs" not in dir(remote_storage):
        raise RuntimeError("Current storage does not support refs")

    # check if that branch exist in remote
    if not remote_storage.have_refs([branch])[0]:
        raise RuntimeError(f'Branch "{branch}" does not exist on remote')

    # get the current commit
    current_commit = (
        get_ref(branch, ref_remotes_dir) if is_ref(branch, ref_remotes_dir) else ""
    )

    local_remote_branch = Path(ref_remotes_dir, branch)
    local_remote_branch.parent.mkdir(parents=True, exist_ok=True)
    remote_storage.get_refs([branch], [str(local_remote_branch)])

    # get the latest commit
    latest_commit = (
        get_ref(branch, ref_remotes_dir) if is_ref(branch, ref_remotes_dir) else ""
    )

    if current_commit == latest_commit:
        return False

    if remote_path == local_path:
        if current_commit == latest_commit:
            return False
        return True

    local_storage = get_backend(local_path)

    dirs, objects = [], []
    commit = latest_commit
    commit_branches = []
    while commit and commit != current_commit:
        if local_storage.have_commits(hash_values=[commit])[0]:
            if commit_branches:
                commit = commit_branches[0]
                commit_branches = commit_branches[1:]
                continue
            else:
                break

        tmp_path = str(Path("/tmp", commit))  # @PRIORITY2: support cache
        remote_storage.get_commits(hash_values=[commit], paths=[tmp_path])
        local_storage.store_commits(paths=[tmp_path], hash_values=[commit])
        with open(tmp_path, "r") as fi:
            commit_obj = yaml.safe_load(fi)
            if isinstance(commit_obj["prev"], str):
                commit = commit_obj["prev"]
            else:
                commit = commit_obj["prev"][0]
                commit_branches += commit_obj["prev"][1:]

            for _, dir_hash in commit_obj["tracks"].items():
                if dir_hash:
                    dirs.append(dir_hash)
        os.unlink(tmp_path)

    dirs = list(set(dirs))
    while dirs:
        new_dirs = []

        exists = local_storage.have_dirs(dirs)
        to_migrate = [dirs[idx] for idx in range(len(dirs)) if not exists[idx]]
        if not to_migrate:
            break

        # @PRIORITY2: support cache to not relying on `/tmp`
        tmp_paths = [str(Path("/tmp", each)) for each in to_migrate]
        remote_storage.get_dirs(hash_values=to_migrate, paths=tmp_paths)
        local_storage.store_dirs(paths=tmp_paths, hash_values=to_migrate)

        for each in tmp_paths:
            # @PRIORIT2 more dedicated dirs function. This is basically a modification
            # of god.commits.base.get_files_hashes_in_commit_dir
            with open(each, "r") as fi:
                lines = fi.read().splitlines()
            for each_line in lines:
                components = each_line.split(",")
                if components[1] == "d":
                    new_dirs.append(components[-1])
                else:
                    objects.append(components[-1])
            os.unlink(each)

        dirs = list(set(new_dirs))

    objects = list(set(objects))
    exists = local_storage.have_objects(objects)
    to_migrate = [objects[idx] for idx in range(len(objects)) if not exists[idx]]
    if to_migrate:
        tmp_paths = [str(Path("/tmp", each)) for each in to_migrate]
        remote_storage.get_objects(hash_values=to_migrate, paths=tmp_paths)
        local_storage.store_objects(paths=tmp_paths, hash_values=to_migrate)
        for each in tmp_paths:
            os.unlink(each)

    return True
