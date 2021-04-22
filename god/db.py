import hashlib
import os
from pathlib import Path
import sqlite3

from constants import BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR, DB_DIR


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


def get_directory_hash(directory, db_name='main.db'):
    """Get directory hash

    Usually, the directory corresponds to a directory.

    # Args
        directory <str>: the name of the directory (relative path)
        db_name <str>: name of database storing directory detail

    # Returns
        <str>: hash if the directory exist, else ""
    """
    con = sqlite3.connect(DB_DIR / db_name)
    cur = con.cursor()

    result = cur.execute(
            f'SELECT hash FROM dirs '
            f'WHERE path = "{directory}"')

    result = result.fetchall()
    if result:
        return result[0][0]

    return ""


def is_directory_maintained(directory, timestamp, db_name='main.db'):
    """Check if a directory is the same (based on timestamp)

    # Args
        directory <str>: the name of the directory
        timestamp <int>: timestamp, based on `st_mtime`
        db_name <str>: name of database storing directory detail

    # Returns
        <bool>: True if the directory is the same, else False
    """
    con = sqlite3.connect(DB_DIR / db_name)
    cur = con.cursor()

    result = cur.execute(
            f'SELECT timestamp FROM dirs '
            f'WHERE path = "{directory}"')

    result = result.fetchall()[0]
    if timestamp > result:
        return False

    return True




if __name__ == '__main__':
    # create_sqlite()
    create_index_sqlite_db()
