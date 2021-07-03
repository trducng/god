"""Record-related operations

These operations handle:

- Constructing sql logs
- Maintaining sql logs
- Update sql logs
"""
import re
import sqlite3
from pathlib import Path
from collections import defaultdict

from god.commit import get_transform_operations
from god.records.logs import construct_transformation_logs
from god.records.records import Records


def record_add(record_path, config, commit, commit_dir, commit_dirs_dir):
    """Construct sql logs for `records`

    # Args:
        record_path <str>: the path of record database
        config <{}>: the record configuration
        commit <str>: the hash of target commit
        commit_dir <str|Path>: the path to commit directory
        commit_dirs_dir <str|Path>: the path to dirs directory
    """

    with Records(record_path, config) as record:
        if not record.is_existed():
            record.create_index_db()

        record_entries = record.load_record_db_into_dict()
        commit1 = record.get_record_commit()
        file_add, file_remove = get_transform_operations(
            commit1, commit, commit_dir, commit_dirs_dir
        )

        sql_commands = construct_transformation_logs(
            file_add, file_remove, record_entries, config
        )

    return sql_commands
