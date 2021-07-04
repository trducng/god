import os
import re
import sqlite3
from pathlib import Path

from god.core.conf import settings


def get_files(path, recursive=False):
    """Get files in folder `path` (recursively)

    # Args
        path: the relative path to begin checking for files.
        recursive <bool>: find nonsymlinks recursively in sub-directories

    # Returns
        <[Paths]>: list of paths to inside `path`
    """
    files = []
    for child in os.scandir(path):
        if child.is_symlink():
            files.append(child.path)
            continue

        if child.is_dir():
            if child.name == ".god":
                continue
            if recursive:
                files += get_files(child.path, recursive=recursive)
        else:
            files.append(child.path)

    return files


def get_standard_index(config):
    """Get standard index from config

    # Args
        config <{}> the index config
    """

    if len(config) == 1:
        # if there's only 1 kind of index table, retrieve it
        return list(config.keys())[0]

    for key, value in config.items():
        if value.get("STANDARD", None):
            return key

    # else return the first index in config
    return list(config.keys())[0]


def update(target, operation, config, index=None, **kwargs):
    """Update the feature attribute

    # Args
        operation <str>: either add (+) or remove (-)
        target <[str]>: list of files or folders to apply
        config <{}>: the index configuration
        index <str>: the index name to search, otherwise the default one in config
        **kwargs <{}>: the column and value to update
    """
    if index is None:
        index = get_standard_index(config)

    if isinstance(target, str):
        target = [target]

    # collect files
    files = []
    for each in target:
        # expand to files only
        if "*" in each:
            files += list(Path(settings.DIR_CWD).glob(each))
            continue

        each = Path(settings.DIR_CWD, each)
        if each.is_symlink():
            files.append(each)
            continue

        if each.is_file():
            files.append(each)
            continue

        if each.is_dir():
            files += get_files(each, recursive=True)

    # get id of files
    pattern = re.compile(config[index]["PATTERN"])
    file_ids = []
    for fn in files:
        if fn == ".godconfig.yml":
            continue
        match = pattern.match(str(Path(fn).relative_to(settings.DIR_BASE)))
        if match is None:
            continue
        file_ids.append(match.group("id"))
    file_ids = tuple(file_ids)

    # construct remove statement
    sql_statements = []
    if operation == "remove":
        for key, value in kwargs.items():
            for file_id in file_ids:
                sql_statements.append(
                    f'DELETE FROM {key} WHERE id = "{file_id}" AND value = "{value}"'
                )
        con = sqlite3.connect(str(Path(settings.DIR_INDEX, index)))
        cur = con.cursor()

        for sql_statement in sql_statements:
            cur.execute(sql_statement)
        con.commit()
        con.close()

        return sql_statements

    # construct add (+) sql statements
    # check for duplicate values
    con = sqlite3.connect(str(Path(settings.DIR_INDEX, index)))
    cur = con.cursor()
    dup_check = {}
    for key, value in kwargs.items():
        file_vals = cur.execute(
            f"SELECT id FROM {key} "
            f'WHERE value = "{value}" '
            f'AND id IN ({", ".join("?" for _ in file_ids)})',
            file_ids,
        )
        dup_check[(key, value)] = set(file_ids).difference(
            set(each[0] for each in file_vals)
        )

    sql_statements = []
    for (column, value), file_ids in dup_check.items():
        for file_id in file_ids:
            sql_statements.append(
                f'INSERT INTO {column} (id, value) VALUES ("{file_id}", "{value}")'
            )

    # execute statement
    for sql_statement in sql_statements:
        cur.execute(sql_statement)
    con.commit()
    con.close()

    return sql_statements
