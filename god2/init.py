"""Inititate the repo
"""
import sqlite3
from pathlib import Path

from god.constants import DIR_DB, DIR_GOD, DIR_INDEX, DIR_LOG, DIR_MAIN, DIR_OBJ


def init(path):
    """Initiate the repo

    This operation construct the tracking .god directory. The directory contains:
        - Place to stores:
            - hash objects
            - log histories
            - db knowledge
            - commits
    """
    path = Path(path).resolve()

    Path(path, DIR_GOD).mkdir(parents=True, exist_ok=True)
    Path(path, DIR_OBJ).mkdir(parents=True, exist_ok=True)
    Path(path, DIR_MAIN).mkdir(parents=True, exist_ok=True)
    Path(path, DIR_INDEX).mkdir(parents=True, exist_ok=True)
    Path(path, DIR_LOG).mkdir(parents=True, exist_ok=True)
    Path(path, DIR_DB).mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    path = "."
    init(path)
