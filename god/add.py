"""Prepare repository for commit

Behaviors:
    - Add files to staging area
    - Construct new record collections -> maybe need interactive
    - Delete record collections -> maybe need interactive
    - Update record collections -> maybe need interactive
    - Add/Update/Remove records to/from record collection
    -> We need to understand about the working area for records
"""
import subprocess

from tqdm import tqdm

from god.branches.trackchanges import track_working_changes
from god.core.index import Index
from god.records.operations import copy_tree


def add(fds, index_path, dir_obj, base_dir, dir_cache_records, dir_records):
    """Add the files, directories & all records to staging area.

    Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dir_obj <str>: the path to object directory
        base_dir <str>: project base directory
        dir_cache_records <str>: directory containing working records
        dir_records <str>: directory containing to-be-committed records
    """
    add, update, remove, reset_tst, unset_mhash = track_working_changes(
        fds, index_path, base_dir
    )
    # @TODO: hook1: track-working changes -> might need hook here
    # seems to hook to clean up the variables `add`, `update`,...
    # decide the config format (should be YAML like)

    # @TODO: move files to cache, create symlink

    for fp, fh, _ in tqdm(add + update):
        # @TODO: construct descriptor (as json)
        # @TODO: encrypt and compress file, calculate hash
        # @TODO: save descriptor
        # @TODO: upload the files to storage
        # @TODO: suppose that we get the storage implementation from config, but we
        # should get this knowledge from some place like plugins manager and config
        p = subprocess.run(["god-storage-s3", "store-file", fp, fh])
        if p.returncode:
            raise RuntimeError(f"Error during adding file: {p.stderr}")
        # @TODO: delete the files to save space

    # @TODO: hook2: before update index

    # update the index
    with Index(index_path) as index:

        # update files
        index.update(
            add=add,
            update=update,
            remove=remove,
            reset_tst=reset_tst,
            unset_mhash=unset_mhash,
        )

        # move records to staging
        current_records = index.get_records()
        records_update = []
        for rn, rh, rmh, rwh, rm in current_records:
            if rwh == rmh:
                continue
            records_update.append((rn, rwh))
            copy_tree(rwh, dir_cache_records, dir_records)

        index.update_records(update=records_update)
    # @TODO: hook3: after update index
