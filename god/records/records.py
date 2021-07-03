import sqlite3
from pathlib import Path

from god.records.configs import get_columns_and_types


class Records(object):
    """Record table holding data information

    This class provides record table management:
        - create record database
        - migrate schema
        - get information
        - query record database

    # Args:
        record_path <str>: place holding the record table
        record_config <{}>: configuration defining the config
    """

    def __init__(self, record_path, record_config):
        """Initialize the record"""
        self._record_path = record_path
        self._record_config = record_config
        self.con, self.cur = None, None

    def start(self):
        """Start sqlite3 session"""
        if self.con is None:
            self.con = sqlite3.connect(self._record_path)
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

    def load_record_db_into_dict(self):
        """Load record DB into dictionary

        # Returns:
            <{id: {cols: values}}>: the record database
        """
        db_result = self.cur.execute("SELECT * FROM main LIMIT 0")
        cols = [each[0] for each in db_result.description]
        id_idx = cols.index("id")
        cols = [cols[id_idx]] + cols[:id_idx] + cols[id_idx + 1 :]

        db_result = self.cur.execute("SELECT * FROM main")
        db_result = db_result.fetchall()

        result = {}
        for each_db_result in db_result:
            result[each_db_result[0]] = {
                key: value for key, value in zip(cols[1:], each_db_result[1:])
            }

        return result

    def create_index_db(self):
        """Create SQL database

        # Returns:
            <[str]>: list of column names
        """
        cols, col_types = get_columns_and_types(self._record_config)

        sql = [
            f"{col} {col_type}"
            for (col, col_type) in zip(cols, col_types)
            if col_types != "MANY"
        ]

        sql = ", ".join(sql)
        sql = f"CREATE TABLE main({sql})"
        self.cur.execute(sql)

        sql = "CREATE TABLE depend_on(commit_hash text)"
        self.cur.execute(sql)

        for col, col_type in zip(cols, col_types):
            if col_type == "MANY":
                self.cur.execute(f"CREATE TABLE {col}(id TEXT, value TEXT)")

        self.con.commit()

        return cols

    def is_existed(self):
        """Check if the record exists"""
        if not Path(self._record_path).is_file():
            return False

        result = self.cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='main'"
        ).fetchall()
        if not result:
            return False

        return True

    def get_record_commit(self):
        """Get the commit hash that the index database points to

        # Returns
            <str>: the commit hash that index database points to. None if nothing
        """
        result = self.cur.execute("SELECT commit_hash FROM depend_on").fetchall()
        if result:
            return result[0][0]
