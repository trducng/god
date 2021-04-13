"""Inititate the repo
"""

from constants import BASE_DIR, GOD_DIR, HASH_DIR
from pathlib import Path


def init():
    """Initiate the repo

    This operation construct the tracking .god directory
    """
    Path(GOD_DIR).mkdir(parents=True, exist_ok=True)
    Path(HASH_DIR).mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':
    init()

