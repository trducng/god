"""Inititate the repo"""
from pathlib import Path

from god.constants import (
    DEFAULT_DIR_OBJECTS,
    DIR_COMMITS,
    DIR_COMMITS_DIRECTORY,
    DIR_GOD,
    DIR_RECORDS,
    DIR_RECORDS_CACHE,
    DIR_RECORDS_DB,
    DIR_RECORDS_LOG,
    DIR_REFS,
    DIR_REFS_HEADS,
    DIR_SNAPS,
    FILE_CONFIG,
    FILE_HEAD,
    FILE_INDEX,
)
from god.exceptions import RepoExisted
from god.index import create_blank_index


def repo_exists(path):
    """Check if the repository exists

    # Args:
        path <str|Path>: the path to repository

    # Exception:
        RepoExisted: if any of the main file and folder already exist
    """
    if Path(path, DIR_GOD).is_dir():
        raise RepoExisted(f"`{DIR_GOD}` directory already exists")

    if Path(path, FILE_CONFIG).is_file():
        raise RepoExisted(f"`{FILE_CONFIG}` file already exists")


def init(path):
    """Initiate the repo

    This operation construct the tracking .god directory. The `.god` repository
    structure is as follows:
        .god/
            - HEAD - store the pointers
            - index - the index file for checking diff
            - config - the config file
            - objects/ - store hashed objects for version control
            - commits/ - store the commits
            - records/ - store the records
            - snaps/ - store the snapshots
            - refs/ - store branch references for commits and records
        .godconfig - the common local config for everyone to follow

    The initialization process initializes `.god` and `.godconfig`.

    # Args
        path <str|Path>: the path to set up repository
    """
    path = Path(path).resolve()

    # Create directory structure
    Path(path, DIR_GOD).mkdir(parents=True, exist_ok=True)

    Path(path, DIR_COMMITS).mkdir(parents=True, exist_ok=True)
    Path(path, DIR_COMMITS_DIRECTORY).mkdir(parents=True, exist_ok=True)

    Path(path, DIR_RECORDS).mkdir(parents=True, exist_ok=True)
    Path(path, DIR_RECORDS_LOG).mkdir(parents=True, exist_ok=True)
    Path(path, DIR_RECORDS_DB).mkdir(parents=True, exist_ok=True)
    Path(path, DIR_RECORDS_CACHE).mkdir(parents=True, exist_ok=True)

    Path(path, DIR_REFS).mkdir(parents=True, exist_ok=True)
    Path(path, DIR_REFS_HEADS).mkdir(parents=True, exist_ok=True)

    Path(path, DIR_SNAPS).mkdir(parents=True, exist_ok=True)

    Path(path, DEFAULT_DIR_OBJECTS).mkdir(parents=True, exist_ok=True)

    # Create file
    with Path(path, FILE_HEAD).open("w") as f_out:
        f_out.write({"REFS": "main"})
    create_blank_index(Path(path, FILE_INDEX))


if __name__ == "__main__":
    path = "."
    init(path)
