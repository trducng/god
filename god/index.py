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

        if name != ".":
            conditions.append(f"(name='{name}' OR name LIKE '{name}/%')")

        conditions = " AND ".join(conditions)
        conditions = f" WHERE {conditions}" if conditions else ""

        sql = f"SELECT * FROM dirs{conditions}"

        return self.cur.execute(sql).fetchall()

    def get_files_info(self, files=None, get_remove=True, not_in=False):
        """Get files inside an index

        # Args
            files <[str]>: list of file names (excluding path)
            get_remove <bool>: whether to retrieve files marked as removed
            not_in <bool>: only available when `files` is not None, if True,
                retrieve all filse that are not `files`

        # Returns
            <[(str, str, str, int, str, str, int, float)]>: name, hash, mhash, remove,
                shash, smash, sremove, timestamp
        """
        conditions = []
        if files is not None:
            if isinstance(files, str):
                files = [files]
            exclude = "NOT IN" if not_in else "IN"
            conditions.append(f"name {exclude} ({','.join(['?'] * len(files))})")

        if not get_remove:
            conditions.append("(remove IS NULL OR NOT remove=1)")

        conditions = f" WHERE {' AND '.join(conditions)}" if conditions else ""

        sql = f"SELECT * FROM dirs{conditions}"
        if files is not None:
            result = self.cur.execute(sql, files)
        else:
            result = self.cur.execute(sql)

        return result.fetchall()

    def update(
        self,
        add=[],
        update=[],
        remove=[],
        reset_tst=[],
        unset_mhash=[],
        unset_remove=[],
        delete=[],
        new_entries=[]
    ):
        """Update the `index`

        # Args:
            add <[str, str, float]>: name, mhash, tstamp
            update <[str, str, float]>: name, mhash, tstamp
            remove <[str]>: name
            reset_tst <[str, float]>: name, tstamp
            unset_mhash <[str]>: name
            unset_remove <[str]>: name
            delete <[str]>: name of entries to delete
            new_entries <[str, str, float]>: name, hash, tstamp
        """
        if unset_mhash:
            self.cur.execute(
                f"UPDATE dirs SET mhash=NULL WHERE "
                f"name IN ({','.join(['?'] * len(unset_mhash))})",
                unset_mhash,
            )

        if remove:
            self.cur.execute(
                f"UPDATE dirs SET remove=1 WHERE "
                f"name IN ({','.join(['?'] * len(remove))})",
                remove,
            )

        if delete:
            self.cur.execute(
                f"DELETE FROM dirs WHERE "
                f"name in ({','.join(['?'] * len(delete))})",
                delete
            )

        if unset_remove:
            self.cur.execute(
                f"UPDATE dirs SET remove=NULL WHERE "
                f"name IN ({','.join(['?'] * len(unset_remove))})",
                unset_remove,
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

        for fn, fh, tst in new_entries:
            self.cur.execute(
                f"INSERT INTO dirs (name, hash, tstamp) VALUES (?, ?, ?)",
                (fn, fh, tst),
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
