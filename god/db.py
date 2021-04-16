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
    cursor.execute("CREATE TABLE main(hash text, path text)")

    # Collect files
    files = get_nonsymlinks(BASE_DIR)

    # Calculate hash
    for each_file in files:
        with open(each_file, 'rb') as f_in:
            file_hash = hashlib.sha256(f_in.read()).hexdigest()
            file_path = Path(each_file).relative_to(BASE_DIR)
            cursor.execute(
                f"INSERT INTO main VALUES('{file_hash}', '{file_path}')")

    con.commit()
    con.close()


if __name__ == '__main__':
    create_sqlite()
