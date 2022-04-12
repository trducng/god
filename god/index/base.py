"""Index-related functionality

Create plugin manifest.
"""
import sqlite3
from pathlib import Path
from typing import List, Tuple

from god.utils.exceptions import FileExisted

COLUMNS = [
    "name text",
    "hash text",
    "mhash text",
    "loc text",
    "mloc text",
    "remove integer",
    "mtime real",
    "ignore integer",
]


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

    def build(self, force: bool = False) -> None:
        """Create the blank index

        Args:
            force: if True, force create if the index database already exists
        """
        if Path(self._index_path).exists():
            if force:
                Path(self._index_path).unlink()
            else:
                raise FileExisted(
                    f"Index exists at {self._index_path}, cannot create new index. "
                    "Enable `force` argument to override this error."
                )

        con = sqlite3.connect(str(self._index_path))
        cur = con.cursor()

        cur.execute(f'CREATE TABLE main({", ".join(COLUMNS)})')
        cur.execute("CREATE INDEX index_main ON main(name)")

        con.commit()
        con.close()

    def unbuild(self) -> None:
        """Delete the index"""
        index = Path(self._index_path)
        if not index.exists():
            raise ValueError(f"{self._index_path} does not exist")
        index.unlink()

    def get_files(self, names: List[str], get_remove: bool, not_in: bool) -> List:
        """Get files inside an index

        @TODO: during working on track working or staging changes, combine this get
        files with get folders.

        Args:
            names: list of file names or a folder name
            get_remove: whether to include files marked as removed
            not_in: retrieve all files that are not in `names`

        Returns:
            <[(str, str, str, int, float, int)]>: name, hash, mhash, remove, mtime,
                ignore
        """
        conditions = []
        if names:
            if isinstance(names, str):
                names = [names]
            exclude = "NOT IN" if not_in else "IN"
            conditions.append(f"name {exclude} ({','.join(['?'] * len(names))})")

        if not get_remove:
            conditions.append("(remove IS NULL OR NOT remove=1)")

        conditions = f" WHERE {' AND '.join(conditions)}" if conditions else ""

        sql = f"SELECT * FROM main{conditions}"
        if names is not None:
            result = self.cur.execute(sql, names)
        else:
            result = self.cur.execute(sql)

        return result.fetchall()

    def get_folder(self, names: List[str], get_remove: bool):
        """Get files inside folder in an index

        Args:
            names: folder name
            get_remove <bool>: whether to get entries marked as remove

        Returns:
            <[(str, str, str, int, float, int)]>: name, hash, mhash, remove, mtime,
                ignore
        """
        conditions = []
        if not get_remove:
            conditions.append("(NOT remove=1 OR remove IS NULL)")

        if "." not in names:
            name_conditions = []
            for name in names:
                name_conditions.extend([f"name='{name}'", f"name LIKE '{name}/%'"])
            if name_conditions:
                conditions.append(f"({' OR '.join(name_conditions)})")

        conditions = " AND ".join(conditions)
        conditions = f" WHERE {conditions}" if conditions else ""

        sql = f"SELECT * FROM main{conditions}"

        return self.cur.execute(sql).fetchall()

    def update(self, items: List[Tuple[str, str, float]]) -> None:
        """Update the index"""
        for name, mhash, mtime, mloc in items:
            self.cur.execute(
                f"UPDATE main SET mhash='{mhash}', mtime={mtime}, mloc='{mloc}' WHERE name='{name}'"
            )
        self.con.commit()

    def revert(self, items: List[Tuple[str, float]], mhash: bool, remove: bool):
        """Revert from staging

        @TODO: construct an auto solution where the method just need filename, and
        it can find the timestamp, whether to uncheck mhash or remove.
        """
        if mhash:
            for name, mtime in items:
                self.cur.execute(
                    f"UPDATE main SET mhash=NULL, mloc=NULL, mtime={mtime} WHERE name='{name}'",
                )

        if remove:
            for name, mtime in items:
                self.cur.execute(
                    f"UPDATE main SET remove=NULL, mtime={mtime} WHERE name='{name}'",
                )

        if not (mhash or remove):
            for name, mtime in items:
                self.cur.execute(
                    f"UPDATE main SET mtime={mtime} WHERE name='{name}'",
                )

        self.con.commit()

    def add(self, items: List[Tuple[str, str, float, str]], staged: bool) -> None:
        """Add the entry to index

        Args:
            items: Each item contains name, mhash, tstamp and mloc
            staged: if True, add hash to hash rather than mhash
        """
        h = "mhash" if staged else "hash"
        l = "mloc" if staged else "loc"  # noqa: E741
        for name, mhash, mtime, mloc in items:
            self.cur.execute(
                f"INSERT INTO main (name, {h}, mtime, {l}) VALUES (?, ?, ?, ?)",
                (name, mhash, mtime, mloc),
            )
        self.con.commit()

    def delete(self, items: List[str], staged: bool) -> None:
        """Delete entries from index

        Args:
            items: each item contains the name
            staged: if True, just mark entries as remove, rather than actually deleting
                the entries from the index
        """
        if staged:
            self.cur.execute(
                "UPDATE main SET remove=1 WHERE "
                f"name IN ({','.join(['?'] * len(items))})",
                items,
            )
        else:
            self.cur.execute(
                f"DELETE FROM main WHERE " f"name in ({','.join(['?'] * len(items))})",
                items,
            )
        self.con.commit()
