from pathlib import Path
from typing import Dict, Union

import god.utils.constants as c

_MUST_EXIST = [c.DIR_GOD, c.FILE_HEAD]


def get_base_dir(path=None) -> str:
    """Get `god` base dir from `path`

    # Args
        path <str>: the directory

    # Returns
        <str>: the directory that contains `.god` directory
    """
    if path is None:
        path = Path.cwd().resolve()

    current_path = Path(path).resolve()

    while True:
        fail = False
        for each_must in _MUST_EXIST:
            if not (current_path / each_must).exists():
                fail = True
                break

        if fail:
            if current_path.parent == current_path:
                # this is root directory
                raise RuntimeError("Uninitialized god repo. Please run `god init`")
            current_path = current_path.parent

        else:
            return str(current_path)


def plugin_endpoints(name: str, base_dir: Union[str, Path] = None) -> Dict[str, str]:
    """Get plugin index-path, track-path, untrack-path, cache-path

    Returns:
        [str]: index path
        [str]: track directory
        [str]: untrack directory
        [str]: cache directory
        [str]: the base directory
    """
    base_dir = Path(get_base_dir(path=base_dir))
    result = {
        "index": str(base_dir / c.DIR_INDICES / name),
        "tracks": str(base_dir / c.DIR_HIDDEN_WORKING / name / "tracks"),
        "untracks": str(base_dir / c.DIR_HIDDEN_WORKING / name / "untracks"),
        "cache": str(base_dir / c.DIR_CACHE / name),
        "base_dir": str(base_dir),
    }

    if name == "files":
        result["tracks"] = str(base_dir)

    return result
