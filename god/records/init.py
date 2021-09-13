from pathlib import Path

from god.core.index import Index
from god.records.operations import copy_tree
from god.records.storage import prolly_create
from god.utils.constants import RECORDS_INTERNALS, RECORDS_LEAVES


def init(name: str, index_path: str, dir_cache_records: str, dir_records: str) -> None:
    """Initialize the index

    Example:
        $ god records init <records-name>
        $ god commit

    After initialize the records <records-name>:
        - The blank storage is created in cache storage directory
        - The blank storage is copyed into records directory
        - The entry is created in the storage `index`, ready for commit

    Args:
        name: the name of the records
        index_path: the path to index file
        dir_cache_records: the path to store objects
        dir_records: place to store records
    """
    with Index(index_path) as index:
        _name = index.get_records(name)
        if _name:
            raise AttributeError(f"Records {name} already exists")

        root: str = prolly_create(
            records={},  # empty records
            tree_dir=Path(dir_cache_records, RECORDS_INTERNALS),
            leaf_dir=Path(dir_cache_records, RECORDS_LEAVES),
        )
        copy_tree(root=root, dir_cache=dir_cache_records, dir_records=dir_records)
        index.update_records(add=[(name, root)])
