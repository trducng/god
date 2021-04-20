import hashlib
import os
from pathlib import Path
import sqlite3

from constants import BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR


def get_nonsymlinks(root):
    """Get non-symlink files in folder `root` (recursively)

    # Args
        root: the path to begin checking for files.

    # Returns
        <[Paths]>: list of paths to non-symlink files
    """
    non_links = []
    for child in os.scandir(root):
        if child.is_symlink():
            continue

        if child.is_dir():
            if child.name == '.god':
                continue
            non_links += get_nonsymlinks(child)
        else:
            non_links.append(child.path)

    return non_links


def create_sqlite():
    con = sqlite3.connect(str(Path(MAIN_DIR, 'main.db')))
    cursor = con.cursor()
    cursor.execute("CREATE TABLE main(path text, hash text)")

    # Collect files
    record_name = '8204ef54fe8e9ef1f893ac73bb41b30c78602f406182897aa778e06833e4aa05'
    with Path(MAIN_DIR, record_name).open('r') as f_in:
        lines = f_in.read().splitlines()
        for each_line in lines:
            fp, fh = each_line.split(' ')
            cursor.execute(
                    f"INSERT INTO main VALUES('{fp[1:]}', '{fh}')")

    con.commit()
    con.close()


def create_index_sqlite_db():
    con = sqlite3.connect(str(Path(MAIN_DIR, 'index.db')))
    cursor = con.cursor()
    cursor.execute("CREATE TABLE main(path text, hash text)")

    # Collect files
    record_name = '8204ef54fe8e9ef1f893ac73bb41b30c78602f406182897aa778e06833e4aa05'
    with Path(MAIN_DIR, record_name).open('r') as f_in:
        lines = f_in.read().splitlines()
        for each_line in lines:
            fp, fh = each_line.split(' ')
            cursor.execute(
                    f"INSERT INTO main VALUES('{fp[1:]}', '{fh}')")

    cursor.execute("CREATE INDEX index_main ON main (path)")

    con.commit()
    con.close()

if __name__ == '__main__':
    # create_sqlite()
    create_index_sqlite_db()
