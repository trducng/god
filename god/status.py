import json
import subprocess


def status(fds, index_path, base_dir):
    """Track statuses of the directories

    # Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        base_dir <str>: project base directory
    """
    p = subprocess.Popen(
        ["god-index", "track", "files"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    out, _ = p.communicate(input=json.dumps(fds).encode())
    return json.loads(out)
