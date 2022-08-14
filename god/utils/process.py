import json
import subprocess
import sys
from io import TextIOWrapper
from typing import Dict, List, Tuple, Union

_JSON = Union[Dict, List, Tuple, None]


def communicate(command: List[str], stdin: _JSON = None) -> _JSON:
    """Communicate to different process

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

    if out:
        return json.loads(out)


def delegate(command: List[str], stdin: _JSON = None) -> _JSON:
    """Delegate running command to different process

    Args:
        command: the shell command to invoke
        stdin: the stdin to the child process (default None)

    Returns:
        The JSON-deserialized output from child process

    Raises:
        RuntimeError: if the child process statuscode is non-zero
    """
    if stdin is None:
        p = subprocess.run(command)
    else:
        p = subprocess.Popen(command, stdin=subprocess.PIPE)
        p.communicate(input=json.dumps(stdin).encode())
        p.wait()


def error(message: str, statuscode: int):
    """Print CLI error message and exit

    Args:
        message: the message to print to stderr
        statuscode: the exit code
    """
    print(message, file=sys.stderr)
    sys.exit(statuscode)


def str_stdin_option(x) -> str:
    """click type check to support both `echo "string" | exe` and `exe --opt "string"`

    Args:
        x: the object passed from command line

    Returns:
        String representation
    """
    if isinstance(x, TextIOWrapper):
        return x.read().strip()

    if x is None:
        return ""

    return str(x)
