import json
import subprocess
from pathlib import Path

from god.records.constants import RECORDS_INTERNALS, RECORDS_LEAVES, RECORDS_ROOT
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
        base_dir: the directory to store this records
        force: force recreate if the records already exists
    """
    track_dir = Path(base_dir, name)
    if track_dir.is_dir():
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

    track_dir.mkdir(parents=True, exist_ok=True)
    (track_dir / RECORDS_INTERNALS).mkdir()
    (track_dir / RECORDS_LEAVES).mkdir()

    # update the config
    print("Please run `god configs edit --level shared --plugin records` to edit info")

    # create empty tree
    root: str = prolly_create(
        records={},  # empty records
        tree_dir=str(track_dir / RECORDS_INTERNALS),
        leaf_dir=str(track_dir / RECORDS_LEAVES),
    )

    with Path(track_dir, RECORDS_ROOT).open("w") as fo:
        fo.write(root)
