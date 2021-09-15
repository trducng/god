"""Prepare repository for commit

Behaviors:
    - Add files to staging area
    - Construct new record collections -> maybe need interactive
    - Delete record collections -> maybe need interactive
    - Update record collections -> maybe need interactive
    - Add/Update/Remove records to/from record collection
    -> We need to understand about the working area for records
"""
from god.branches.trackchanges import track_working_changes
from god.core.files import copy_objects_with_hashes
from god.core.index import Index
from god.records.operations import copy_tree


def add(fds, index_path, dir_obj, base_dir, dir_cache_records, dir_records):
    """Add the files, directories & all records to staging area.

    Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dr_obj <str>: the path to object directory
        base_dir <str>: project base directory
        dir_cache_records <str>: directory containing working records
        dir_records <str>: directory containing to-be-committed records
    """
    add, update, remove, reset_tst, unset_mhash = track_working_changes(
        fds, index_path, base_dir
    )

    # copy files to objects directory
    copy_objects_with_hashes([(each[0], each[1]) for each in add], dir_obj, base_dir)
    copy_objects_with_hashes([(each[0], each[1]) for each in update], dir_obj, base_dir)

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
