import json
import subprocess

from god.core.files import resolve_paths


def status(fds, base_dir):
    """Track statuses of the directories

    # Args:
        fds <str>: the directory to add (absolute path)
        base_dir <str>: project base directory
    """
    fds = resolve_paths(fds, base_dir)
    p = subprocess.Popen(
        ["god-index", "track", "files"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    out, _ = p.communicate(input=json.dumps(fds).encode())
    return json.loads(out)
