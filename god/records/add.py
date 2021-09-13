from pathlib import Path

from god.core.index import Index
from god.records.operations import copy_tree
from god.utils.constants import RECORDS_INTERNALS, RECORDS_LEAVES
from god.utils.exceptions import RecordNotExisted


def add(name: str, index_path: str, dir_cache_records: str, dir_records: str) -> None:
    """Add the records from working condition to staging condition

    Example:
        $ god records add <records-name>
        $ god commit

    This operation:
        1. Copy the tree from cache dir to record dir
        2. Add the working hash whash to staging hash mhash

    Args:
        name: the name of the record
        indx_path: the path to index file
    """
    with Index(index_path) as index:
        records = index.get_records(name=name)
        if not records:
            raise RecordNotExisted(f"Record {name} not existed")
        rn, rh, rmh, rwh, _ = records[0]
        copy_tree(root=rwh, dir_cache=dir_cache_records, dir_records=dir_records)
        index.update_records(update=[(rn, rwh)])
