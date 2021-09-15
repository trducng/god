"""Index-related functionality"""
import sqlite3
from collections import defaultdict
from pathlib import Path

from god.utils.exceptions import FileExisted

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

INDEX_RECORD_COLS = [
    "name text",  # record name
    "hash text",  # commited root
    "mhash text",  # staged root
    "whash text",  # working root value
    "remove integer",  # whether the record is removed
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

    # files index
    cur.execute(f'CREATE TABLE dirs({", ".join(INDEX_DIRECTORY_COLS)})')
    cur.execute("CREATE INDEX index_dirs ON dirs(name)")

    # records index
    cur.execute(f'CREATE TABLE records({", ".join(INDEX_RECORD_COLS)})')

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
        new_entries=[],
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
                f"DELETE FROM dirs WHERE " f"name in ({','.join(['?'] * len(delete))})",
                delete,
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
                "INSERT INTO dirs (name, mhash, tstamp) VALUES (?, ?, ?)",
                (fn, mfh, tst),
            )

        for fn, fh, tst in new_entries:
            self.cur.execute(
                "INSERT INTO dirs (name, hash, tstamp) VALUES (?, ?, ?)",
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
                "INSERT INTO dirs (name, hash, tstamp) VALUES (?, ?, ?)",
                (fn, fh, tst),
            )
        self.con.commit()

    def to_files_hashes(self):
        """Get active files

        # Returns:
            <{fp: fh}>: file path and file hashes
        """
        files_hashes = {}
        files_info = self.get_files_info(get_remove=False)
        for f in files_info:
            fh = f[2] or f[1]
            files_hashes[f[0]] = fh

        return files_hashes

    def to_files_dirs_hashes(self):
        """Get active files organized by directory

        # Returns:
            <{dir_path: [(fn, fh)]}>: file path and file hashes
        """
        file_dirs_hashes = defaultdict(list)
        files_info = self.get_files_info(get_remove=False)
        for f in files_info:
            fp = Path(f[0])
            fh = f[2] or f[1]
            file_dirs_hashes[str(fp.parent)].append((fp.name, fh))

        return file_dirs_hashes

    def update_records(
        self,
        add: list = [],
        update: list = [],
        delete: list = [],
        update_whash: list = [],
    ) -> None:
        """Add records to index

        Args:
            add: each item contains (name, root-hash)
            update: each item contains (name, mhash)
            delete: each item is a record name
        """
        for rn, rmh in update:
            self.cur.execute(f"UPDATE records SET mhash='{rmh}' WHERE name='{rn}'")

        for rn, rwh in update_whash:
            self.cur.execute(f"UPDATE records SET whash='{rwh}' WHERE name='{rn}'")

        for rn, rh in add:
            self.cur.execute(
                "INSERT INTO records (name, mhash, whash) VALUES (?, ?, ?)",
                (rn, rh, rh),
            )

        if delete:
            self.cur.execute(
                f"DELETE FROM records WHERE "
                f"name in ({','.join(['?'] * len(delete))})",
                delete,
            )

        self.con.commit()

    def reconstruct_records(self, records: list) -> None:
        """Construct the index based

        Args:
           records: each item contains record name, hash and whash
        """
        # reset the table
        self.cur.execute("DELETE FROM records")
        self.con.commit()

        # construct records
        for rn, rh, rwh in records:
            self.cur.execute(
                "INSERT INTO records (name, hash, whash) VALUES (?, ?, ?)",
                (rn, rh, rwh),
            )
        self.con.commit()

    def get_records(self, name: str = None) -> list:
        """Get records information

        Args:
            name: if yes, filter by name

        Returns:
            Each item contains (name, hash, mhash, whash, remove)
        """
        conditions = f" WHERE name='{name}'" if name else ""
        sql = f"SELECT name, hash, mhash, whash, remove FROM records{conditions}"
        result = self.cur.execute(sql)

        return result.fetchall()
