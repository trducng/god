import json
import shutil
import subprocess
from pathlib import Path

from god.records.constants import RECORDS_INTERNALS, RECORDS_LEAVES, RECORDS_TRACKS
from god.records.storage import prolly_create


def init(name: str, base_dir: str, force: bool) -> None:
    """Initialize the record <name>

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
    if Path(base_dir, RECORDS_TRACKS, name).exists():
        if not force:
            raise ValueError(
                f'Record "{name}" already exists. To override, try again with `force`'
            )
        else:
            p = subprocess.Popen(
                ["god-index", "delete", "records", "--staged"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )
            _, _ = p.communicate(input=json.dumps([name]).encode())

    # create empty tree
    root: str = prolly_create(
        records={},  # empty records
        tree_dir=str(Path(base_dir, RECORDS_INTERNALS)),
        leaf_dir=str(Path(base_dir, RECORDS_LEAVES)),
    )

    shutil.copy(
        Path(base_dir, RECORDS_INTERNALS, root),
        Path(base_dir, RECORDS_TRACKS, name),
    )
