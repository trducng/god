import json
import subprocess
import sys
from typing import Dict, List, Union

_JSON = Union[Dict, List, None]


def communicate(command: List[str], stdin: _JSON = None) -> _JSON:
    """Communicate to dufferent process

    Args:
        command: the shell command to invoke
        stdin: the stdin to the child process (default None)

    Returns:
        The JSON-deserialized output from child process

    Raises:
        RuntimeError: if the child process statuscode is non-zero
    """
    if stdin is None:
        p = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.stdout, p.stderr
    else:
        p = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = p.communicate(input=json.dumps(stdin).encode())
        p.wait()

    if p.returncode != 0:
        raise RuntimeError(f"{' '.join(command)} fails with {err}")

    if p.stdout:
        return json.loads(out)


def error(message: str, statuscode: int):
    """Print CLI error message and exit

    Args:
        message: the message to print to stderr
        statuscode: the exit code
    """
    print(message, file=sys.stderr)
    sys.exit(statuscode)
