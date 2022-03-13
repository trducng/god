import json
import subprocess


def status() -> tuple:
    """Check the status of records

    Given that the records config are not changed, this command check for:

    Args:
        index_path: the path to index file

    Returns:
        []: stage add - list of records are added in staging
        []: stage update - list of records are updated in staging
        []: stage remove - list of records are removed in staging
        []: add - list of records that are added in working dir but not staged
        []: update - list of records that are updated in working dir but not staged
    """
    p = subprocess.Popen(
        ["god-index", "track", "records"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    out, _ = p.communicate(input=json.dumps(["."]).encode())
    return json.loads(out)
