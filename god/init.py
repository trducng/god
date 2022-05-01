"""Inititate the repo"""
import json
import subprocess
from pathlib import Path

import god.utils.constants as c
from god.configs.init import init as configs_init
from god.plugins.init import init as plugins_init
from god.plugins.install import construct_working_directory, create_blank_index
from god.storage.utils import DEFAULT_STORAGE
from god.utils.exceptions import RepoExisted


def repo_exists(path):
    """Check if the repository exists

    # Args:
        path <str|Path>: the path to repository

    # Exception:
        RepoExisted: if any of the main file and folder already exist
    """
    if Path(path, c.DIR_GOD).is_dir():
        raise RepoExisted(f"`{c.DIR_GOD}` directory already exists")

    if Path(path, c.FILE_CONFIG).is_file():
        raise RepoExisted(f"`{c.FILE_CONFIG}` file already exists")


def init(path):
    """Initiate the repo

    @PRIORITY2: refactor the docstring
    This operation construct the tracking .god directory. The `.god` repository
    structure is as follows:
        .god/
            - HEAD - store the pointers
            - index - the index file for checking diff
            - config - the config file
            - objects/ - store hashed objects for version control
            - commits/ - store the commits
            - refs/ - store branch references for commits and records
            - .godconfig
        .godconfig - the common local config for everyone to follow

    The initialization process initializes `.god` and `.godconfig`.

    # Args
        path <str|Path>: the path to set up repository
    """
    path = Path(path).resolve()

    # Create directory structure
    for each_var in dir(c):
        if "DIR" in each_var:
            Path(path, getattr(c, each_var)).mkdir(parents=True, exist_ok=True)

    # Create file
    with Path(path, c.FILE_HEAD).open("w") as f_out:
        json.dump({"REFS": "main"}, f_out)
    subprocess.run(["god-index", "build", "files"])

    # Setup configs
    working_dir = construct_working_directory("configs")
    create_blank_index("configs")
    configs_init(str(working_dir), False)

    # Setup plugins
    working_dir = construct_working_directory("plugins")
    create_blank_index("plugins")
    plugins_init(str(working_dir), False)

    # Setup default local storage
    with open(c.FILE_LINK, "w") as fo:
        json.dump({"STORAGE": DEFAULT_STORAGE, "REMOTES": []}, fo)


if __name__ == "__main__":
    path = "."
    init(path)
