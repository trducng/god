"""Represent the record as sqlite database table

This database reflects the working area of records, not the commit area.

This component will be represented as hook. As a result, it does not assume the same
knowledge about context as inner functions.

At the moment, we aren't sure where the plugins code should reside, just assume that
from perspective of the script, it is run inside the repository.

We would like this operation to be performed inside a plugin in the future. So assume
so.
"""
import csv
import re
import sqlite3
from collections import defaultdict
from io import StringIO
from pathlib import Path

import click

from god.core.common import get_base_dir
from god.core.index import Index
from god.records.configs import RecordsConfig
from god.records.constants import RECORDS_INTERNALS, RECORDS_LEAVES
from god.records.storage import get_records
from god.utils.constants import DIR_CACHE_DB, DIR_CACHE_RECORDS, FILE_CONFIG, FILE_INDEX


class SQLiteTable:
    """Record SQL table holding data information

    This class provides record table management:
        - create record database
        - migrate schema -> don't need to migrate schema. This operation is stateless.
        - query record database

    # Args:
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
        """Construct record database based on the provided information"""
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

    def create_record_db(self, config: RecordsConfig):
        """Create SQL database

        # Returns:
            <[str]>: list of column names
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
    ):
        records = get_records(
            root=root,
            tree_dir=Path(dir_record, RECORDS_INTERNALS),
            leaf_dir=Path(dir_record, RECORDS_LEAVES),
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

    def search(self, queries: list, columns: list) -> list:
        """Get the commit hash that the index database points to

        # Returns
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


@click.group()
def db():
    pass


@db.command("post-commit")
def post_commit():
    """Run this hook during post commit

    In this command, it will:
        - Get the current index file
            + Obtain the list of records: name and root hash
            + Obtain the list of files and hashes
        - Construct the records according to that information for each record
    """
    base_dir = get_base_dir()
    file_index = str(Path(base_dir, FILE_INDEX))
    dir_db = str(Path(base_dir, DIR_CACHE_DB))
    dir_record = Path(base_dir, DIR_CACHE_RECORDS)
    file_config = Path(base_dir, FILE_CONFIG)

    # get necessary records and files information
    with Index(file_index) as index:
        files_info = index.get_files_info()
        records_info = index.get_records()
    files_hashes = {_[0]: _[1] or _[2] for _ in files_info}

    # construct the db
    for record_info in records_info:
        record_name = record_info[0]
        root_hash = record_info[3]
        with SQLiteTable(dir_db, record_name) as table:
            table.construct_record(
                config=RecordsConfig(records_name=record_name, config_path=file_config),
                files_hashes=files_hashes,
                root=root_hash,
                dir_record=dir_record,
            )


@db.command("search")
@click.argument("name")
@click.option(
    "-q", "--query", "queries", multiple=True, type=str, help="Search query (col=val)"
)
@click.option(
    "-c", "--col", "columns", multiple=True, type=str, help="Column to return"
)
def search(name: str, queries: list, columns: list):
    base_dir = get_base_dir()
    dir_db = str(Path(base_dir, DIR_CACHE_DB))
    with SQLiteTable(dir_db=dir_db, name=name) as table:
        items = table.search(queries, columns)

    buff = StringIO()
    writer = csv.writer(buff)
    writer.writerows(items)
    print(buff.getvalue())
