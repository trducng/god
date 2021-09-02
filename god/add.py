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
from god.commits.compare import transform_commit
from god.core.files import copy_objects_with_hashes
from god.core.index import Index
from god.records.logs import construct_transformation_logic

# from god.records.records import Records


# def construct_record(record_path, config, commit, commit_dir, commit_dirs_dir):
#     """Construct sql logs for `records`

#     Args:
#         record_path <str>: the path of record database
#         config <{}>: the record configuration
#         commit <str>: the hash of target commit
#         commit_dir <str|Path>: the path to commit directory
#         commit_dirs_dir <str|Path>: the path to dirs directory
#     """
#     with Records(record_path, config) as record:
#         if not record.is_existed():
#             record.create_index_db()

#         record_entries = record.load_record_db_into_dict()
#         commit1 = record.get_record_commit()
#         file_add, file_remove = transform_commit(
#             commit1, commit, commit_dir, commit_dirs_dir
#         )

#         sql_commands = construct_transformation_logic(
#             file_add, file_remove, record_entries, config
#         )

#     return sql_commands


def add(fds, index_path, dir_obj, base_dir):
    """Add the files and directories to staging area. Construct records if necessary.

    Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dr_obj <str>: the path to object directory
        base_dir <str>: project base directory
    """
    add, update, remove, reset_tst, unset_mhash = track_working_changes(
        fds, index_path, base_dir
    )

    # check if the .godconfig has changed
    # if changed, check if it relates to .RECORDS
    # if relates to .RECORDS, check if this is about:
    # - Add new record collection?
    #   + Init in index from past commits, and newly created files
    # - Delete record collection?
    # - Update record collection?
    new_records = []  # start with lame-ass check TODO improve later
    rename_records = []
    removed_records = []

    # if not changed, add files as normal -> don't need to add files

    # copy files to objects directory
    copy_objects_with_hashes([(each[0], each[1]) for each in add], dir_obj, base_dir)
    copy_objects_with_hashes([(each[0], each[1]) for each in update], dir_obj, base_dir)

    # copy records into records directory

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

        # make any necessary adjustment index regarding records
        # so that the commit process only needs to copy
        # in this case:
        #   1. create a new records
        #   2. register into index
        for records_name, records_root in new_records:
            index.add_records(records_name, records_root)
