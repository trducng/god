import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Union

_JSON = Union[Dict, List, None]


def binary_search(item, values: list, start: int = None, end: int = None) -> int:
    """Retrieve the index of `item` in list of `values`

    This function assumes that `values` is sorted in increasing order

    Args:
        item: the item to search
        values: the list of item to search from
        start: the start index from `values` to search
        end: the end index from `values` to search

    Returns:
        the index of `item` in `values` or None if not found
    """
    start = 0 if start is None else start
    end = len(values) - 1 if end is None else end

    if values[start] > item:
        return -1

    if values[end] < item:
        return -1

    if (start == end) or (end == start + 1):
        if values[start] == item:
            return start
        if values[end] == item:
            return end
        return -1

    middle = (start + end) // 2
    if values[middle] == item:
        return middle
    elif values[middle] > item:
        return binary_search(item, values, start, middle)
    else:
        return binary_search(item, values, middle, end)


def one_line_sorted_json(items: list) -> str:
    """Encode the list of dictionary separated by newline

    Purposes:
        - human readable: separating each dictionary inside `items` by newline
        - unique: sort the dictionary keys for uniqueness

    Args:
        items: the list of dictionaries content

    Returns:
        the string representation of `items` that can be written to a text file as
            valid JSON
    """
    result = ["["]
    if items:
        for item in items[:-1]:
            result.append(f"{json.dumps(item, sort_keys=True)},")
        result.append(f"{json.dumps(items[-1], sort_keys=True)}")
    result.append("]")

    return "\n".join(result)


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

    if p.stdout:
        return json.loads(out)


def get_endpoints() -> Dict[str, str]:
    """Get records endpoints

    Returns:
        Endpoints, which are: index, tracks, untracks, cache, base_dir
    """
    return communicate(
        ["god", "plugins", "info", "-n", "records", "--endpoints", "--json"],
    )  # type: ignore


def error(message: str, statuscode: int):
    """Print CLI error message and exit

    Args:
        message: the message to print to stderr
        statuscode: the exit code
    """
    print(message, file=sys.stderr)
    sys.exit(statuscode)


def resolve_paths(fds, base_dir) -> List[str]:
    """Resolve path relative to `base_dir`

    Args:
        fds <[str]>: list of absolute paths
        base_dir <str|Path>: the repo base directory

    Returns
        <[str]>: list of relative path to `base_dir`
    """
    base_dir = Path(base_dir).resolve()
    return [str(Path(each).resolve().relative_to(base_dir)) for each in fds]


def list_records() -> List[str]:
    """List the name of all records

    Returns:
        Name of all records
    """
    tracks_dir = Path(get_endpoints()["tracks"])
    result = []
    for each_dir in tracks_dir.glob("*/"):
        result.append(each_dir.name)

    return result
