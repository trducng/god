"""Inititate the repo"""
import json
from pathlib import Path

import god.utils.constants as c
from god.configs import update_config
from god.plugins.base import initiate_plugin
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
        json.dump({"REFS": "main", "EXPOSED_PLUGINS": "files"}, f_out)

    initiate_plugin("files", path)
    initiate_plugin("plugins", path)
    initiate_plugin("configs", path)

    # Default configs
    update_config(
        plugin="configs",
        level="local",
        config_dict={"storage": DEFAULT_STORAGE, "remotes": {}, "default_remote": ""},
        base_dir=str(path),
    )
