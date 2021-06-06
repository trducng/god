"""Index-related functionality"""
import sqlite3
from pathlib import Path

from god.exceptions import FileExisted
from god.files import get_string_hash


INDEX_DIRECTORY_COLS = [
    "name text",  # directory name
    "hash text",  # directory hash (calculated by files content)
    "mhash text",  # directory hash (modified by files content)
    "tstamp real",  # last modified time
    "remove integer",  # removed or not
]


def create_blank_index(path):
    """Create a blank index

    # Args:
        path <str>: the path to index file
    """
    if Path(path).exists():
        raise FileExisted(f"index exists at {path}, cannot create new index")

    con = sqlite3.connect(str(path))
    cur = con.cursor()

    cur.execute(f'CREATE TABLE dirs({", ".join(INDEX_DIRECTORY_COLS)})')
    cur.execute("CREATE INDEX index_dirs ON dirs(name)")

    con.commit()
    con.close()


def create_index(path, commit_id):
    """Create the index from commit_id"""
    pass


class Index:
    """The index file"""

    def __init__(self, index_path):
        """Initialize the index path"""
        self._index_path = index_path
        self.con, self.cur = None, None

    def start(self):
        """Start sqlite3 connection"""
        if self.con is None:
            self.con = sqlite3.connect(self._index_path)
            self.cur = self.con.cursor()

    def stop(self):
        """Stop sqlite3 connection"""
        if self.con is not None:
            self.con.close()
            self.con, self.cur = None, None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def get_files(self, dir_hash, files=None, get_remove=True):
        """Get files from db with hash

        # Args:
            dir_hash <str>: the directory hash
            files <[str]>: list of file names (excluding path)
            get_remove <bool>: whether to retrieve files marked as removed

        # Returns
            <[(str, str, str, float)]>: filename, hash, mhash, timestamp
        """
        conditions = []
        if files:
            if isinstance(files, str):
                files = [files]
            conditions.append(f"name IN ({','.join(['?'] * len(files))})")

        if not get_remove:
            conditions.append("NOT remove=1")

        conditions = f" WHERE {' AND '.join(conditions)}" if conditions else ""

        sql = f"SELECT * FROM dirs_{dir_hash}{conditions}"
        if files is None:
            result = self.cur.execute(sql)
        else:
            result = self.cur.execute(sql, files)

        return result.fetchall()

    def get_dir_hash(self, dir_name):
        """Get the directory hash from directory name

        # Args:
            dir_name <str>: the name of the directory

        # Returns
            <str>: the hash, or "" if the directory does not exist
            <str>: the mhash, or "" if the directory does not exist
        """
        sql = f"SELECT hash, mhash FROM dirs WHERE name='{dir_name}'"
        result = self.con.execute(sql).fetchall()
        if result:
            return result[0]

        return "", ""

    def create_files_table(self, files, dir_name, dir_tst, modified=False):
        """Create files table

        # Args:
            files <[(str, str, float)>: each contain name, hash and tstamp
            dir_name <float>: directory name
            dir_tst <float>: directory modified time
            modified <bool>: whether the `hashes` are in the hash or mhash col
        """
        files = sorted(files, key=lambda obj: obj[0])
        dir_hash = get_string_hash("\n".join(",".join(each[:-1]) for each in files))

        hash_col = "mhash" if modified else "hash"
        tb_name = f"dirs_{dir_hash}"

        self.cur.execute(f"CREATE TABLE {tb_name}({', '.join(INDEX_DIRECTORY_COLS)})")
        self.cur.execute(f"CREATE INDEX index_{dir_hash} ON {tb_name} (name)")
        self.con.commit()

        for fn, fh, tst in files:
            self.cur.execute(
                f"INSERT INTO {tb_name} (name, {hash_col}, tstamp) VALUES (?, ?, ?)",
                (fn, fh, tst)
            )
        self.cur.execute(
            f"INSERT INTO dirs (name, mhash, tstamp) VALUES (?, ?, ?)",
            (dir_name, dir_hash, dir_tst)
        )
        self.con.commit()

    def update_files_tables(self, add_files, update_files, remove_files, dir_name, dir_tst):
        """Update the files tables

        # Args:
            add_files <[(str, str, float)>: each contain name, hash and tstamp
            update_files <[(str, str, float)>: each contain name, hash and tstamp
            remove_files <[str]>: each contain name
            dir_hash <str>: the directory hash
            dir_tst <str>: the directory modified time
        """
        dir_hash, dir_mhash = self.get_dir_hash(dir_name)
        dhash = dir_mhash or dir_hash
        tb_name = f"dirs_{dhash}"

        for fn, mfh, tst in add_files:
            self.cur.execute(
                f"INSERT INTO {tb_name} (name, mhash, tstamp) VALUES (?, ?, ?)",
                (fn, mfh, tst)
            )

        for fn, mfh, tst in update_files:
            self.cur.execute(
                f"UPDATE {tb_name} SET mhash='{mfh}', tstamp={tst} WHERE name='{fn}'"
            )

        if remove_files:
            self.cur.execute(
                f"UPDATE {tb_name} SET remove=1 WHERE "
                f"name IN ({','.join(['?'] * len(remove_files))})",
                remove_files,
            )

        self.cur.execute(f"ALTER TABLE {tb_name} RENAME TO {tb_name}_")
        self.con.commit()

        # recalculate table name
        current_files = self.get_files(f"{dhash}_", get_remove=False)

        files_hashes = []
        for current_file in current_files:
            # get current hash
            fh = current_file[2] or current_file[1]
            files_hashes.append((current_file[0], fh))
        files_hases = sorted(files_hashes, key=lambda obj: obj[0])
        new_dir_hash = get_string_hash(
            "\n".join(",".join(each) for each in files_hashes)
        )

        # change table name
        self.cur.execute(
            f"UPDATE dirs SET mhash='{new_dir_hash}', tstamp={dir_tst} "
            f"WHERE name='{dir_name}'"
        )
        self.cur.execute(f"ALTER TABLE {tb_name}_ RENAME TO dirs_{new_dir_hash}")
        self.cur.execute(f"DROP INDEX index_{dhash}")
        self.cur.execute(f"CREATE INDEX index_{new_dir_hash} ON dirs_{new_dir_hash} (name)")
        self.con.commit()

    def _close_context(self):
        self.con.close()
        self.con, self.cur = None, None
