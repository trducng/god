"""Inititate the repo
"""
import sqlite3
from pathlib import Path

from constants import (BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR, LOG_DIR, DB_DIR, MAIN_DB)


def init():
    """Initiate the repo

    This operation construct the tracking .god directory. The directory contains:
        - Place to stores:
            - hash objects
            - log histories
            - db knowledge
            - commits
        - Empty DB
    """
    Path(GOD_DIR).mkdir(parents=True, exist_ok=True)
    Path(HASH_DIR).mkdir(parents=True, exist_ok=True)
    Path(MAIN_DIR).mkdir(parents=True, exist_ok=True)
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    Path(DB_DIR).mkdir(parents=True, exist_ok=True)

    # create db
    con = sqlite3.connect(str(Path(DB_DIR, MAIN_DB)))
    cur = con.cursor()
    cur.execute("CREATE TABLE dirs(path text, hash text, timestamp int)")
    con.commit()
    con.close()


if __name__ == '__main__':
    init()

