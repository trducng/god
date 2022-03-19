import json
import subprocess


def add(name: str) -> None:
    """Add the configs from working condition to staging condition

    Example:
        $ god configs add <configs-name>
        $ god commit

    This operation:
        1. Copy the tree from cache dir to record dir
        2. Add the working hash whash to staging hash mhash

    Args:
        name: the name of the record
        indx_path: the path to index file
    """
    p = subprocess.Popen(
        ["god-index", "track", "configs", "--working"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, _ = p.communicate(input=json.dumps([name]).encode())
    add, update, remove, reset_tst, unset_mhash = json.loads(out)

    # @TODO: construct Storage & Descriptor for configs

    # update the index
    if unset_mhash:
        p = subprocess.Popen(
            ["god-index", "revert", "configs", "--mhash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(unset_mhash).encode())

    if reset_tst:
        p = subprocess.Popen(
            ["god-index", "revert", "configs"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(reset_tst).encode())

    if remove:
        p = subprocess.Popen(
            ["god-index", "delete", "configs", "--staged"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(remove).encode())

    if update:
        p = subprocess.Popen(
            ["god-index", "update", "configs"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(update).encode())

    if add:
        p = subprocess.Popen(
            ["god-index", "add", "configs", "--staged"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(add).encode())
