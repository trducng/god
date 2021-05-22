"""Inititate the repo
"""
import sqlite3
from pathlib import Path

from god.base import (get_base_dir, GOD_DIR, OBJ_DIR, MAIN_DIR, LOG_DIR, DB_DIR)


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

    Path(path, GOD_DIR).mkdir(parents=True, exist_ok=True)
    Path(path, OBJ_DIR).mkdir(parents=True, exist_ok=True)
    Path(path, MAIN_DIR).mkdir(parents=True, exist_ok=True)
    Path(path, LOG_DIR).mkdir(parents=True, exist_ok=True)
    Path(path, DB_DIR).mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    path = '.'
    init(path)
