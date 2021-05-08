import hashlib
import os
from pathlib import Path
import sqlite3

from constants import BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR, DB_DIR
from history import get_current_db


def get_connection_cursor(db_name):
    con = sqlite3.connect(str(Path(DB_DIR, db_name)))
    return con, con.cursor()


def create_index_db(directories):
    """Construct index directory for commit

    # Args
        directories <[(str, str)]>: directory path and hash

    # Returns
        <str>: the hash name of the index db
    """
    directories = sorted(directories, key=lambda obj: obj[0])
    directories_text = [','.join(each) for each in directories]

    db_name = hashlib.sha256('\n'.join(directories_text).encode()).hexdigest()
    db_path = Path(DB_DIR, db_name)
    if db_path.is_file():
        return db_name

    # Construct the index database
    con, cur = get_connection_cursor(db_name)
    cur.execute('CREATE TABLE dirs(path text, hash text, timestamp real)')
    for path, dh in directories:
        timestamp = Path(BASE_DIR, path).stat().st_mtime
        cur.execute(f'INSERT INTO dirs VALUES("{path}", "{dh}", "{timestamp}")')
    cur.execute('CREATE INDEX index_main ON dirs (path)')

    # Construct history database
    cur.execute('CREATE TABLE depend_on(hash text)')
    cur.execute(f'INSERT INTO depend_on VALUES("{get_current_db()}")')

    con.commit()
    con.close()

    return db_name


def create_directory_db(files):
    """Create a directory database

    The directory database is a SQLite database that has name equal to the hash of
    sorted <file_name>,<file_hash> in the folder. Each row in the database has format:
        - <file_name1>, <file_hash1>
        - <file_name2>, <file_hash2>

    (the file_name does not have parent detail)

    # Args
        files <[(str, str)]>: filename and file hash

    # Returns
        <str>: the hash name of the directory db
    """
    files = [(str(Path(fp).name), fh) for (fp, fh) in files]
    files = sorted(files, key=lambda obj: obj[0])
    files_text = [','.join(each) for each in files]

    db_name = hashlib.sha256('\n'.join(files_text).encode()).hexdigest()
    db_path = Path(DB_DIR, db_name)
    if db_path.is_file():
        # Likely merely changing folder name
        return db_name

    # Construct the database
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute("CREATE TABLE dirs(path text, hash text)")

    # Populate the entries
    for file_name, file_hash in files:
        cur.execute(f'INSERT INTO dirs VALUES("{file_name}", "{file_hash}")')

    cur.execute('CREATE INDEX index_main ON dirs (path)')
    con.commit()
    con.close()

    return db_name


def get_directory_hash(directory):
    """Get directory hash

    Usually, the directory corresponds to a directory.

    # Args
        directory <str>: the name of the directory (relative path)
        db_name <str>: name of database storing directory detail

    # Returns
        <str>: hash if the directory exist, else ""
    """
    db_name = get_current_db()

    con = sqlite3.connect(str(DB_DIR / db_name))
    cur = con.cursor()

    result = cur.execute(
            f'SELECT hash FROM dirs '
            f'WHERE path = "{directory}"')

    result = result.fetchall()
    if result:
        return result[0][0]

    return ""


def is_directory_maintained(directory, timestamp):
    """Check if a directory is the same (based on timestamp)

    # Args
        directory <str>: the name of the directory
        timestamp <int>: timestamp, based on `st_mtime`
        db_name <str>: name of database storing directory detail

    # Returns
        <bool>: True if the directory is the same, else False
    """
    db_name = get_current_db()

    con = sqlite3.connect(str(DB_DIR / db_name))
    cur = con.cursor()

    result = cur.execute(
            f'SELECT timestamp FROM dirs '
            f'WHERE path = "{directory}"')

    result = result.fetchall()[0][0]
    if timestamp > result:
        return False

    return True


def get_sub_directory(directory, recursive=False):
    """Get recorded sub-directories of `directory`

    # Args
        directory <str>: the name of the directory
        db_name <str>: name of database storing directory detail
        recursive <bool>: whether to look for sub-directories recursively

    # Returns
        <[str]>: list of sub-directories (relative to BASE_DIR)
    """
    directory = str(directory)
    db_name = get_current_db()

    con = sqlite3.connect(str(DB_DIR / db_name))
    cur = con.cursor()

    if directory == '.':
        result = cur.execute('SELECT path FROM dirs')
    else:
        result = cur.execute('SELECT path FROM dirs WHERE path LIKE "{directory}%"')
    result = [each[0] for each in result.fetchall()]

    if recursive:
        return result

    return [
        each for each in result
        if str(Path(each).parent) == directory
    ]


def get_untouched_directories(directories):
    """Get untouched directories

    # Args:
        directories <[str]>: list of directory to exclude

    # Returns
        <[str]>: list of untouched directories
    """
    db_name = get_current_db()

    con = sqlite3.connect(str(DB_DIR / db_name))
    cur = con.cursor()

    sql = 'SELECT path, hash FROM dirs WHERE path NOT IN ({})'.format(
        ','.join(['?'] * len(directories)))
    result = cur.execute(sql, directories)

    return result.fetchall()


def get_file_hash(file_name, cursor):
    """Check if file exists

    # Args
        file_name <str>: the file to check
        cursor <sqlite3.cursor>: the cursor to check

    # Returns
        <int>: 0 if not exist, 1 if exist,
    """
    result = cursor.execute(f'SELECT path, hash FROM dirs WHERE path = "{file_name}"')
    result = result.fetchall()

    if not result:
        return None

    return result[0][1]


def get_files(db_name):
    """Get the files from db_name"""
    con = sqlite3.connect(str(DB_DIR / db_name))
    cur = con.cursor()

    result = cur.execute(f'SELECT * FROM dirs')
    return result.fetchall()


def get_removed_files(file_names, cursor):
    """Get files in DB that are not in `file_names`

    # Args
        file_names <[str]>: list of file names to ignore
        cursor <sqlite3.cursor>: cursor to the database

    # Returns
        <[(str, str)]>: list of file names and hashes
    """
    if not file_names:
        return []
    sql = 'SELECT path, hash FROM dirs WHERE path NOT IN ({})'.format(
        ','.join(['?'] * len(file_names)))
    result = cursor.execute(sql, file_names)

    return result.fetchall()


if __name__ == '__main__':
    create_index_sqlite_db()
