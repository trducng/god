from pathlib import Path

import god.utils.constants as c

_MUST_EXIST = [c.DIR_GOD, c.FILE_HEAD]
_DEFAULT_CONFIG = {
    "OBJECTS": {
        "STORAGE": "local",
    }
}


def get_base_dir(path=None):
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
                raise RuntimeError("Unitialized god repo. Please run `got init`")
            current_path = current_path.parent

        else:
            return str(current_path)
