"""Porcelain commands to be used with CLI"""
import sqlite3
from pathlib import Path

from god.init import repo_exists, init



def init_cmd(path):
    """Initialize the repository"""
    path = Path(path).resolve()
    repo_exists(path)
    init(path)


def add_cmd(path):
    """Move `path` to staging area, ready for commit"""
    pass


if __name__ == '__main__':
    path = '.'
    init_cmd(path)

