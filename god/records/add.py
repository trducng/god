import json
import subprocess
from pathlib import Path

from god.core.common import get_base_dir
from god.records.descriptors import RecordDescriptor


def add(name: str) -> None:
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
    # hook to organize the files accordingly (preparing the stage for add to do its
    # work)

    p = subprocess.Popen(
        ["god-index", "track", "records", "--working"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # call the command here
    out, _ = p.communicate(input=json.dumps([name]).encode())
    add, update, remove, reset_tst, unset_mhash = json.loads(out)

    working_dir = Path(get_base_dir(), ".god", "workings", "records")
    new_objs = [[fp, fh, str(Path(working_dir, "tracks", fp))] for fp, fh, _ in add] + [
        [fp, fh, str(Path(working_dir, "tracks", fp))] for fp, fh, _ in update
    ]

    for each_tree in new_objs:
        # 0. check for internal / leaf nodes not indexed by storage
        # all_hashes = []
        remaining_hashes = []

        # 1. move all new internal and leaf nodes to storage
        storage_cmd = "god-storage-s3"
        child = subprocess.Popen(
            args=[storage_cmd, "store-files"],
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )
        _, _ = child.communicate(input=json.dumps(remaining_hashes).encode())

        # 2. move the root to storage
        child = subprocess.Popen(
            args=[
                storage_cmd,
                "store-file",
                "--file-path",
                str(each_tree[2]),
                "--file-hash",
                each_tree[1],
            ],
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )

        # 3. create descriptor for the root node
        descriptor = RecordDescriptor.descriptor()
        descriptor["hash"] = "sha256"
        descriptor["checksum"] = each_tree[1]
        descriptor["location"] = each_tree[1]

        # 4. store the descriptor to descriptor-specific location
        _ = subprocess.run(
            args=["god-descriptor", "store-descriptor", json.dumps(descriptor)],
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )

    # update the index
    if unset_mhash:
        p = subprocess.Popen(
            ["god-index", "revert", "records", "--mhash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(unset_mhash).encode())

    if reset_tst:
        p = subprocess.Popen(
            ["god-index", "revert", "records"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(reset_tst).encode())

    if remove:
        p = subprocess.Popen(
            ["god-index", "delete", "records", "--staged"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(remove).encode())

    if update:
        p = subprocess.Popen(
            ["god-index", "update", "records"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(update).encode())

    if add:
        p = subprocess.Popen(
            ["god-index", "add", "records", "--staged"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(add).encode())
