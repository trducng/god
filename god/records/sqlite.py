"""Represent the record as sqlite database table

This database reflects the working area of records, not the commit area.
"""
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Union

from god.records.configs import RecordsConfig
from god.records.constants import RECORDS_INTERNALS, RECORDS_LEAVES
from god.records.storage import get_records


class SQLiteTable:
    """Record SQL table holding data information

    This class provides record table management:
        - create record database
        - migrate schema -> don't need to migrate schema. This operation is stateless.
        - query record database

    Args:
        dir_db: directory to store the cache database
        name: the name of the record, which is also the name of the database
    """

    def __init__(self, dir_db: str, name: str):
        """Initialize the record"""
        self._db_path = str(Path(dir_db, name))
        self._name = name
        self.con, self.cur = None, None

    def start(self):
        """Start sqlite3 session"""
        if self.con is None:
            self.con = sqlite3.connect(self._db_path)
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

    def construct_record(
        self,
        config: RecordsConfig,
        files_hashes: dict,
        root: str,
        dir_record: str,
    ) -> None:
        """Construct record database based on the provided information

        Args:
            config: the records config
            files_hashes: the dictionary of {file-path: file-hash}
            root: hash address of the root tree
            dir_records: directory that store internal and leaf nodes
        """
        # construct database
        self.create_record_db(config)
        # get information for each rows
        result_dict = self.parse_records(config, files_hashes, root, dir_record)
        # update the table
        for id_, cols_values in result_dict.items():
            cols = ["id"] + list(cols_values.keys())
            values = [id_] + list(cols_values.values())
            self.cur.execute(
                f"INSERT INTO main ({', '.join(cols)}) VALUES ({', '.join('?'*len(values))})",
                values,
            )
        self.con.commit()

    def create_record_db(self, config: RecordsConfig) -> List[str]:
        """Create SQL database

        Args:
            config: the records config

        Returns:
            List of column names
        """
        tables = self.cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        for table in tables:
            self.cur.execute(f"DROP TABLE {table[0]}")
        self.con.commit()

        cols, col_types = config.get_columns_and_types()

        sql = [
            f"{col} {col_type}"
            for (col, col_type) in zip(cols, col_types)
            if "ARRAY" not in col_type
        ]
        for path_col in config.get_path_columns():
            sql.append(f"{path_col}_hash TEXT")

        sql = ", ".join(sql)
        sql = f"CREATE TABLE main({sql})"
        self.cur.execute(sql)

        sql = "CREATE TABLE depend_on(commit_hash text)"
        self.cur.execute(sql)

        for col, col_type in zip(cols, col_types):
            if "ARRAY" in col_type:
                self.cur.execute(f"CREATE TABLE {col}(id TEXT, value TEXT)")

        self.con.commit()

        return cols

    def parse_records(
        self, config: RecordsConfig, files_hashes: dict, root: str, dir_record: str
    ) -> Dict:
        """Parse from file paths and records to table entries

        Args:
            config: the records config
            files_hashes: the dictionary of {file-path: file-hash}
            root: hash address of the root tree
            dir_records: directory that store internal and leaf nodes

        Returns:
            Table entries, with format: {id: {col1: val1, col2: val2}}
        """
        records = get_records(
            root=root,
            tree_dir=str(Path(dir_record, RECORDS_INTERNALS)),
            leaf_dir=str(Path(dir_record, RECORDS_LEAVES)),
        )
        pattern = re.compile(config.get_pattern())
        conversion_groups = config.get_group_rule()
        path_cols = set(config.get_path_columns())
        result_dict = defaultdict(dict)  # {id: {col: val}}
        for fn, fh in files_hashes.items():
            match = pattern.match(fn)
            if match is None:
                continue
            match_dict = match.groupdict()

            if "id" not in match_dict:
                continue
            id_ = match_dict.pop("id")

            for group, match_key in match_dict.items():
                if group in conversion_groups:
                    match_value = conversion_groups[group][match_key]
                    result_dict[id_][match_value] = fn
                    group = match_value
                else:
                    result_dict[id_][group] = match_key

                if group in path_cols:
                    result_dict[id_][group + "_hash"] = fh

            if id_ in records:
                result_dict[id_].update(records[id_])

        return result_dict

    def search(self, queries: List, columns: Union[List, Tuple]) -> List:
        """Get the commit hash that the index database points to

        Args:
            queries: the user-supplied queries to run
            columns: the columns to return

        Returns:
            <str>: the commit hash that index database points to. None if nothing
        """
        return_cols = "*" if not columns else ", ".join(columns)
        # @TODO: handle type conversion
        # @TODO: handle many to many
        conditions = []
        for each in queries:
            col, val = each.split("=")
            conditions.append(f"{col}='{val}'")
        conditions = " AND ".join(conditions)
        conditions = f" WHERE {conditions}" if conditions else ""
        sql = f"SELECT {return_cols} FROM main{conditions}"
        result = self.cur.execute(sql)
        columns = tuple([_[0] for _ in result.description])
        return [columns] + result.fetchall()
