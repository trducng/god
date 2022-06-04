"""Index-related functionality
Create plugin manifest.
"""
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

from god.index.utils import COLUMNS
from god.utils.exceptions import FileExisted


class Index:
    """The index file"""

    def __init__(self, index_path):
        """Initialize the index path"""
        self._index_path = index_path
        self.con: sqlite3.Connection = None  # type: ignore
        self.cur: sqlite3.Cursor = None  # type: ignore

    def start(self):
        """Start sqlite3 connection"""
        if self.con is None:
            self.con = sqlite3.connect(self._index_path)
            self.cur = self.con.cursor()

    def stop(self):
        """Stop sqlite3 connection"""
        if self.con is not None:
            self.con.close()
            self.con, self.cur = None, None  # type: ignore

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def save(self, name, **kwargs):
        keys = ["name"] + list(kwargs.keys())
        values = [name] + list(kwargs.values())
        self.cur.execute(
            f"INSERT INTO main ({', '.join(keys)}) "
            f"VALUES ({', '.join(['?'] * len(keys))})",
            values,
        )
        self.con.commit()

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

        cur.execute(
            f'CREATE TABLE main({", ".join(" ".join(each) for each in COLUMNS)})'
        )
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

    def get_folder(self, names: List[str], get_remove: bool, conflict: bool):
        """Get files inside folder in an index

        Args:
            names: folder name
            get_remove: whether to get entries marked as remove
            conflict: whether files should be conflicted

        Returns:
            <[(str, str, str, int, float, int)]>: name, hash, mhash, remove, mtime,
                ignore
        """
        conditions = []
        if not get_remove:
            conditions.append("(NOT remove=1 OR remove IS NULL)")

        if conflict:
            conditions.append("ctheirs IS NOT NULL")

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
        for name, mhash, mtime in items:
            self.cur.execute(
                f"UPDATE main SET mhash='{mhash}', mtime={mtime} WHERE name='{name}'"
            )
        self.con.commit()

    def revert(self, items: List[str], mhash: bool, remove: bool):
        """Revert from staging

        @TODO: construct an auto solution where the method just need filename, and
        it can find the timestamp, whether to uncheck mhash or remove.
        """
        if mhash:
            for name in items:
                self.cur.execute(
                    f"UPDATE main SET mhash=NULL, mtime=NULL WHERE name='{name}'",
                )

        if remove:
            for name in items:
                self.cur.execute(
                    f"UPDATE main SET remove=NULL WHERE name='{name}'",
                )

        # if not (mhash or remove):
        #     for name in items:
        #         self.cur.execute(
        #             f"UPDATE main SET mtime={mtime} WHERE name='{name}'",
        #         )

        self.con.commit()

    def add(self, items: List[Tuple[str, str, float]], staged: bool) -> None:
        """Add the entry to index

        Args:
            items: Each item contains name, mhash, tstamp
            staged: if True, add hash to hash rather than mhash
        """
        if not items:
            return

        h = "mhash" if staged else "hash"
        for name, mhash, mtime in items:
            self.cur.execute(
                f"INSERT INTO main (name, {h}, mtime) VALUES (?, ?, ?)",
                (name, mhash, mtime),
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

    def conflict(self, items: Dict[str, Tuple[str, str]]):
        """Change index according to conflict

        If an entry does not already exist in the index, that entry will be created
        new.

        Args:
            items: each item contains name and 2 conflicts hashes (their hash and
                base hash). If the conflict hash is an empty string, it means the
                file is deleted
        """
        if not items:
            return

        names = list(items.keys())
        existing_entries = [
            each[0]
            for each in self.get_files(names=names, get_remove=True, not_in=False)
        ]
        non_existing_entries = list(set(names).difference(existing_entries))

        for name in existing_entries:
            self.cur.execute(
                f"UPDATE main SET ctheirs='{items[name][0]}', cbase='{items[name][1]}' "
                f"WHERE name='{name}'"
            )

        for name in non_existing_entries:
            self.cur.execute(
                "INSERT INTO main (name, ctheirs, cbase) VALUES (?, ?)",
                (name, items[name][0], items[name][1]),
            )
        self.con.commit()

    def get_conflict_add_add(self, case: int) -> List:
        """Get entries that are updated in both ours and theirs

        Args:
            case: 1 = all, 2 = unresolved, 3 = resolved
        """
        if case == 1:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NOT NULL AND "
                "ctheirs IS NOT NULL AND ctheirs != ''"
            ).fetchall()
        elif case == 2:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NOT NULL AND "
                "ctheirs IS NOT NULL AND ctheirs != '' AND mhash IS NULL"
            ).fetchall()
        elif case == 3:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NOT NULL AND "
                "ctheirs IS NOT NULL AND ctheirs != '' AND "
                "mhash IS NOT NULL AND mhash != ''"
            ).fetchall()

        raise AttributeError(f"Unknown case: {case}")

    def get_conflict_add_remove(self, case: int) -> List:
        """Get entries that are updated in ours but removed in theirs

        Args:
            case: 1 = all, 2 = unresolved, 3 = unresolved ours, 4 = resolved theirs
        """
        if case == 1:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NOT NULL AND ctheirs = ''"
            ).fetchall()
        elif case == 2:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NOT NULL AND ctheirs = '' AND "
                "mhash IS NULL AND remove IS NULL"
            ).fetchall()
        elif case == 3:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NOT NULL AND ctheirs = '' AND "
                "mhash IS NOT NULL"
            ).fetchall()
        elif case == 4:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NOT NULL AND ctheirs = '' AND "
                "remove = 1"
            ).fetchall()

        raise AttributeError(f"Unknown case: {case}")

    def get_conflict_remove_add(self, case: int) -> List:
        """Get entries that are removed in ours, but updated in theirs"""
        if case == 1:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NULL AND "
                "ctheirs IS NOT NULL AND ctheirs != ''"
            ).fetchall()
        elif case == 2:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NULL AND "
                "ctheirs IS NOT NULL AND ctheirs != '' AND "
                "remove IS NULL AND mhash IS NULL"
            ).fetchall()
        elif case == 3:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NULL AND "
                "ctheirs IS NOT NULL AND ctheirs != '' AND remove = 1"
            ).fetchall()
        elif case == 4:
            return self.cur.execute(
                "SELECT * FROM main WHERE hash IS NULL AND "
                "ctheirs IS NOT NULL AND ctheirs != '' AND mhash IS NOT NULL"
            ).fetchall()

        raise AttributeError(f"Unknown case: {case}")

    def step(self):
        """Step from staging to committed

        For non-ignore items, this method essentially:
            - move mhash -> hash
            - remove entries that are removed
        """
        # remove
        self.cur.execute("DELETE FROM main WHERE remove = 1")
        self.con.commit()

        # update
        items = self.cur.execute(
            "SELECT * FROM main WHERE mhash IS NOT NULL"
        ).fetchall()
        # @RUSH: the `items` below doesn't have exe
        for name, _, mhash, _, _, _, _, _, _ in items:
            self.cur.execute(
                f"UPDATE main SET hash='{mhash}', mhash=NULL, ctheirs=NULL, "
                f"cbase=NULL WHERE name='{name}'"
            )
        self.con.commit()
