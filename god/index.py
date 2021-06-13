"""Index-related functionality"""
import sqlite3
from pathlib import Path

from god.exceptions import FileExisted
from god.files import get_string_hash


INDEX_DIRECTORY_COLS = [
    "name text",  # directory name
    "hash text",  # directory hash (calculated by files content)
    "mhash text",  # directory hash (modified by files content)
    "remove integer",  # removed or not
    "shash text",
    "smhash text",
    "sremove integer",
    "tstamp real",  # last modified time
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

    def match(self, name, get_remove=True):
        """Check if a name exists in index

        # Args
            name <str>: the name (relative to BASE_DIR)
            get_remove <bool>: whether to get entries marked as removed

        # Returns
            <[(str, str, str, int, str, str, float)]>: name, hash, mhash, remove,
                shash, smash, sremove, timestamp
        """
        conditions = []
        if not get_remove:
            conditions.append("(NOT remove=1 OR remove IS NULL)")

        if name != '.':
            conditions.append(f"(name='name' OR name LIKE '{name}/%')")

        conditions = " AND ".join(conditions)
        conditions = f" WHERE {conditions}"  if conditions else ""

        sql = f"SELECT * FROM dirs{conditions}"

        return self.cur.execute(sql).fetchall()


    def get_files_info(self, remove=True):
        """Get files inside an index

        # Args
            remove <bool>: whether to retrieve file marked as removed

        # Returns
            <[(str, str, str, int, str, str, int, float)]>: name, hash, mhash, remove,
                shash, smash, sremove, timestamp
        """
        conditions = []
        if not remove:
            conditions.append("NOT remove=1 OR remove IS NULL")

        conditions = " AND ".join(conditions)
        conditions = f" WHERE {conditions}" if conditions else ""

        sql = f"SELECT * FROM dirs{conditions}"
        result = self.cur.execute(sql).fetchall()

        return result

    def update(self, add=[], update=[], remove=[], reset_tst=[], unset_mhash=[]):
        """Update the `index`

        # Args:
            add <[str, str, float]>: name, mhash, tstamp
            update <[str, str, float]>: name, mhash, tstamp
            remove <[str]>: name
            reset_tst <[str, float]>: name, tstamp
            unset_mhash <[str]>: name
        """
        if unset_mhash:
            self.cur.execute(
                f"UPDATE dirs SET mhash=NULL WHERE "
                f"name IN ({','.join(['?'] * len(unset_mhash))})",
                unset_mhash
            )

        if remove:
            self.cur.execute(
                f"UPDATE dirs SET remove=1 WHERE "
                f"name IN ({','.join(['?'] * len(remove))})",
                remove,
            )

        for fn, tst in reset_tst:
            self.cur.execute(f"UPDATE dirs SET tstamp={tst} WHERE name='{fn}'")

        for fn, mfh, tst in update:
            self.cur.execute(
                f"UPDATE dirs SET mhash='{mfh}', tstamp={tst} WHERE name='{fn}'"
            )

        for fn, mfh, tst in add:
            self.cur.execute(
                f"INSERT INTO dirs (name, mhash, tstamp) VALUES (?, ?, ?)",
                (fn, mfh, tst),
            )

        self.con.commit()

    def construct_index_from_files_hashes_tsts(self, files):
        """Construct the index based

        # Args:
            files <[str, str, float]>: relative filepath, hash, timestamp
        """
        # reset the table
        self.cur.execute("DELETE FROM dirs")
        self.con.commit()

        # add new records
        for fn, fh, tst in files:
            self.cur.execute(
                f"INSERT INTO dirs (name, hash, tstamp) VALUES (?, ?, ?)",
                (fn, fh, tst),
            )
        self.con.commit()

