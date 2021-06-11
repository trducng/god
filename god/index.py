"""Index-related functionality"""
import sqlite3
from pathlib import Path

from god.exceptions import FileExisted
from god.files import get_string_hash


OLD_INDEX_DIRECTORY_COLS = [
    "name text",  # directory name
    "hash text",  # directory hash (calculated by files content)
    "mhash text",  # directory hash (modified by files content)
    "tstamp real",  # last modified time
    "remove integer",  # removed or not
]


def create_blank_old_index(path):
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


class IndexOld:
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

    def get_files(self, dir_hash, files=None, get_remove=True, not_in=False):
        """Get files from db with hash

        # Args:
            dir_hash <str>: the directory hash
            files <[str]>: list of file names (excluding path)
            get_remove <bool>: whether to retrieve files marked as removed
            not_in <bool>: only available when `files` is not None, if True,
                retrieve all filse that are not `files`

        # Returns
            <[(str, str, str, float)]>: filename, hash, mhash, timestamp
        """
        conditions = []
        if files:
            if isinstance(files, str):
                files = [files]
            exclude = "NOT IN" if not_in else "IN"
            conditions.append(f"name {exclude} ({','.join(['?'] * len(files))})")

        if not get_remove:
            conditions.append("(remove IS NULL OR NOT remove=1)")

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

    def get_sub_directories(self, directory, recursive=False, get_remove=True):
        """Get record sub-directories in `index`

        # Args:
            directory <str>: the name of the directory (relative to `BASE_DIR`)
            recursive <bool>: whether to look for sub-directories recursively
            get_remove <bool>: whether to retrieve sub-directories marked as removed

        # Returns:
            <[(str, str, str, float, int)]>: directory name, hash, mhash, timestamp,
                remove
        """
        conditions = []
        if not get_remove:
            conditions.append("(remove IS NULL OR NOT remove=1)")

        if directory != ".":
            conditions.append(f"name LIKE '{directory}/%'")

        conditions = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        result = self.cur.execute(f"SELECT * FROM dirs{conditions}").fetchall()

        if recursive:
            return result

        return [each for each in result if str(Path(each[0]).parent) == directory]

    def is_table_exist(self, tb_name):
        """Check if table already exists

        # Args:
            tb_name <str>: the name of table

        # Returns:
            <bool>: True if table exists, False otherwise
        """
        result = self.cur.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tb_name}'"
        ).fetchall()

        if result:
            return True
        return False

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

        if self.is_table_exist(tb_name):
            # if table already exists, then it contains all good files and 
            # TODO: might not be the case, 2 folders can have exactly the same set
            # of files at some point, after that they begin to diverge
            # The previous logic can apply to commit but cannot apply to `index` table
            return

        self.cur.execute(f"CREATE TABLE {tb_name}({', '.join(INDEX_DIRECTORY_COLS)})")
        self.cur.execute(f"CREATE INDEX index_{dir_hash} ON {tb_name} (name)")
        self.con.commit()

        for fn, fh, tst in files:
            self.cur.execute(
                f"INSERT INTO {tb_name} (name, {hash_col}, tstamp) VALUES (?, ?, ?)",
                (fn, fh, tst),
            )
        self.cur.execute(
            f"INSERT INTO dirs (name, mhash, tstamp) VALUES (?, ?, ?)",
            (dir_name, dir_hash, dir_tst),
        )
        self.con.commit()

    def update_files_tables(
        self, add_files, update_files, remove_files, dir_name, dir_tst
    ):
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
                (fn, mfh, tst),
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
        self.cur.execute(
            f"CREATE INDEX index_{new_dir_hash} ON dirs_{new_dir_hash} (name)"
        )
        self.con.commit()

    def update_dirs_tables(self, update_dirs=[], remove_dirs=[]):
        """Update the `dirs` table in `index`

        # Args:
            update_dirs <[(str, str, float)>: each contain name, hash and tstamp
            remove_dirs <[str]>: each contain name
        """
        for dn, mdh, tst in update_dirs:
            self.cur.execute(
                f"UPDATE dirs SET mhash='{mdh}', tstamp={tst} WHERE name='{dn}'"
            )

        if remove_dirs:
            self.cur.execute(
                f"UPDATE dirs SET remove=1 WHERE "
                f"name IN ({','.join(['?'] * len(remove_dirs))})",
                remove_dirs,
            )

        self.con.commit()

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

        # Returns
            <[(str, str, str, int, str, str, float)]>: name, hash, mhash, remove,
                shash, smash, sremove, timestamp
        """
        if name == '.':
            return self.cur.execute("SELECT * FROM dirs").fetchall()

        result = self.cur.execute(
            f"SELECT * FROM dirs WHERE name='name'"
        ).fetchall()

        if result:
            return result

        result = self.cur.execute(
            f"SELECT * FROM dirs WHERE name LIKE '{directory}/%'"
        ).fetchall()

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


if __name__ == "__main__":
    from pprint import pprint

    with Index("/home/john/temp/add_god/index1") as index:
        sub_dirs = index.get_sub_directories("folder1/abc/xyz", recursive=False)
        pprint(sub_dirs)
