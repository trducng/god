import os
from pathlib import Path

from god.commits.base import (
    get_dir_hashes_in_commit,
    get_files_hashes_in_commit,
    get_in_between_commits,
    get_latest_parent_commit,
)
from god.core.refs import get_ref, update_ref
from god.storage.commons import get_backend


def push_ref(
    ref_name: str,
    local_ref_path: str,
    remote_ref_path: str,
    remote_path: str,
    local_path: str,
):
    """Put the ref from local to remote storage, transfer any necessary files"""
    remote_storage = get_backend(remote_path)

    # check if the ref exists on remote
    if not remote_storage.have_refs([ref_name])[0]:
        # if not upload and ok
        remote_commit = ""
    else:
        # if yes, download and check
        tmp_ref = str(Path("/tmp", ref_name))  # @PRIORITY2: use cache
        remote_storage.get_refs(refs=[ref_name], paths=[tmp_ref])
        remote_commit = get_ref(ref_name, "/tmp")

    local_commit = get_ref(ref_name, local_ref_path)  # @PRIORITY2: stupid get_ref
    if remote_commit == local_commit:
        # 1. if the 2 tips are equal: then nothing to upload, ok
        return

    parent_commit = get_latest_parent_commit(remote_commit, local_commit)
    if parent_commit != remote_commit:
        # 2. the remote tip is not parent of local tip, deny, requires pull
        raise RuntimeError("Local and remote diverge. Run `god pull`")

    # 3. the remote tip is a parent of local tip, perform upload
    local_storage = get_backend(local_path)

    # get intermediate commits
    commits = list(set(get_in_between_commits(parent_commit, local_commit)))

    # get directories and objects
    dirs, objects = [], []
    for commit in commits:
        # @PRIORITY1: use all of plugins, rather than files
        objects += list(
            get_files_hashes_in_commit(commit_id=commit, plugin="files").values()
        )
        dirs += list(
            get_dir_hashes_in_commit(commit_id=commit, plugin="files").values()
        )
    objects = list(set(objects))
    dirs = list(set(dirs))

    # upload objects
    exists = remote_storage.have_objects(objects)
    to_migrate = [objects[idx] for idx in range(len(objects)) if not exists[idx]]
    if to_migrate:
        tmp_paths = [str(Path("/tmp", each)) for each in to_migrate]
        local_storage.get_objects(hash_values=to_migrate, paths=tmp_paths)
        remote_storage.store_objects(paths=tmp_paths, hash_values=to_migrate)
        for each in tmp_paths:
            os.unlink(each)

    # upload dirs
    exists = remote_storage.have_dirs(dirs)
    to_migrate = [dirs[idx] for idx in range(len(dirs)) if not exists[idx]]
    if to_migrate:
        tmp_paths = [str(Path("/tmp", each)) for each in to_migrate]
        local_storage.get_dirs(hash_values=to_migrate, paths=tmp_paths)
        remote_storage.store_dirs(paths=tmp_paths, hash_values=to_migrate)
        for each in tmp_paths:
            os.unlink(each)

    # upload commits
    exists = remote_storage.have_commits(commits)
    to_migrate = [commits[idx] for idx in range(len(commits)) if not exists[idx]]
    if to_migrate:
        tmp_paths = [str(Path("/tmp", each)) for each in to_migrate]
        local_storage.get_commits(hash_values=to_migrate, paths=tmp_paths)
        remote_storage.store_commits(paths=tmp_paths, hash_values=to_migrate)
        for each in tmp_paths:
            os.unlink(each)

    # update the index above
    remote_storage.store_refs(
        paths=[str(Path(local_ref_path, ref_name))], refs=[ref_name]
    )
    update_ref(ref_name, local_commit, remote_ref_path)
