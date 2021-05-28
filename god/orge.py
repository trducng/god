import re
import sqlite3
from pathlib import Path
from collections import defaultdict

from god.base import settings, Settings
from god.logs import get_transform_operations


TYPE1 = {
    "NAME": "index",
    "PATTERN": "(?P<input>train1/(?P<id>(?P<label>cat|dog)\.\d+)\..+$)",
    "COLUMNS": {
        "id": "INTEGER",
        "label": "TEXT",
        "input": {
            "path": True
        }
    },
}

TYPE2 = {
    "NAME": "index",
    "PATTERN": "(?P<id>.+?)(_(?P<masktype_>GT\d))?.[^.]+$",
    "COLUMNS": {
        "id": "INTEGER",
        "input": {"path": True, "conversion_group": ("masktype_", None)},
        "horizontal_mask": {"path": True, "conversion_group": ("masktype_", "GT0")},
        "vertical_mask": {"path": True, "conversion_group": ("masktype_", "GT1")},
        "area_mask": {"path": True, "conversion_group": ("masktype_", "GT2")},
        "stamp_mask": {"path": True, "conversion_group": ("masktype_", "GT3")},
        "other_mask": {"path": True, "conversion_group": ("masktype_", "GT4")},
        "features": {"type": "TEXT", "values": ["risk", "no-risk", "some-risk"]},
    },
}

TYPE3 = {
    "NAME": "index",
    "PATTERN": "(?P<id>.+)\.(?P<switch_>.+$)",
    "COLUMNS": {
        "id": "INTERGER",
        "input": {"path": True, "conversion_group": ("switch_", ("png", "jpeg", "jpg"))},
        "label": {"path": True, "conversion_group": ("switch_", "json")},
    }
}


TYPE4 = {
    "NAME": "index",
    "PATTERN": "(?P<class>.+?)/(?P<id>.+\d+)(?P<switch_>.pbdata|/geometry.pbdata|/video.MOV)$",
    "COLUMNS": {
        "id": "INTERGER",
        "class": "TEXT",
        "location": {"path": True, "conversion_group": ("switch_", ".pbdata")},
        "mask_3d": {"path": True, "conversion_group": ("switch_", "/geometry.pbdata")},
        "input": {"path": True, "conversion_group": ("switch_", "/video.MOV")}
    }
}


def get_path_cols(config):
    """Get the group rule

    # Args
        config <{}>: the configuration

    # Returns
    """
    result = []

    COLUMNS = config.get('COLUMNS', {})
    for col_name, col_rule in COLUMNS.items():
        if not isinstance(col_rule, (dict, Settings)):
            continue
        if col_rule.get('path', False) or col_rule.get('PATH', False):
            result.append(col_name)

    return result


def get_group_rule(config):
    """Get the group rule

    # Args
        config <{}>: the configuration

    # Returns
    """
    result = defaultdict(dict)

    COLUMNS = config.get('COLUMNS', {})
    for col_name, col_rule in COLUMNS.items():
        if not isinstance(col_rule, (dict, Settings)):
            continue
        if 'conversion_group' not in col_rule:
            continue

        group_name = list(col_rule['conversion_group'].keys())[0]
        group_val = list(col_rule['conversion_group'].values())[0]
        if not isinstance(group_val, (list, tuple)):
            result[group_name][group_val] = col_name
        else:
            for each_group_val in group_val:
                result[group_name][each_group_val] = col_name

    return result


def get_columns_and_types(config):
    """Get columns and column types from config

    # Args
        config <dict>: orge configuration file

    # Returns
        <[str]>: list of column names
        <[str]>: list of column types
    """
    if not config.get("COLUMNS", []):
        raise RuntimeError('No column specified in "COLUMNS"')

    cols, col_types = [], []

    for key, value in config["COLUMNS"].items():

        if isinstance(value, str):  # col: col_type format
            cols.append(key)
            col_types.append(value)
            continue

        if value.get("path", False):  # path format
            cols += [key, f"{key}_hash"]
            col_types += ["TEXT", "TEXT"]
            continue

        cols.append(key)
        col_types.append(value.get("type", "TEXT"))
        # TODO: handle ManyToMany type

    return cols, col_types


def get_primary_cols(config):
    """Get the primary column in table, if any of these columns is deleted, the entry
    is deleted.

    # Args
        config <dict>: orge configuration file

    # Returns
        <[str]>: list of primary column names
    """
    if not config.get("COLUMNS", []):
        raise RuntimeError('No column specified in "COLUMNS"')

    cols = []

    for key, value in config["COLUMNS"].items():

        if isinstance(value, str):  # col: col_type format
            continue

        if value.get("primary", False):  # path format
            cols.append(key)
            # TODO: raise or ignore if column is ManyToManyType
            continue

    return cols


def get_db_commit(index_db_path):
    """Get the commit hash that the index database points to

    # Args
        index_db_path <str>: the path to index database

    # Returns
        <str>: the commit hash that index database points to. Empty string '' if None
    """
    index_db_path = Path(index_db_path).resolve()
    if index_db_path.exists():
        con = sqlite3.connect(str(index_db_path))
        cur = con.cursor()
        result = cur.execute("SELECT commit_hash FROM depend_on").fetchall()
        con.close()
        if result:
            return result[0][0]

    return ""


def load_index_db(index_db_path):
    """Load index db into dictionary

    # Args
        index_db_path <str>: the path to index database

    # Returns
        <{id: {cols: values}}>: the index database
    """
    con = sqlite3.connect(str(index_db_path))
    cur = con.cursor()

    db_result = cur.execute('SELECT * FROM main LIMIT 0')
    cols = [each[0] for each in db_result.description]
    id_idx = cols.index('id')
    cols = [cols[id_idx]] + cols[:id_idx] + cols[id_idx+1:]

    db_result = cur.execute('SELECT * FROM main')
    db_result = db_result.fetchall()
    con.close()

    result = {}
    for each_db_result in db_result:
        result[each_db_result[0]] = {
                key: value
                for key, value in zip(cols[1:], each_db_result[1:])
        }

    return result


def create_index_db(config, name):
    """Create SQL database from config

    # Args:
        config <dict>: the configuration file
    """
    con = sqlite3.connect(str(Path(settings.DIR_INDEX, name)))
    cur = con.cursor()

    cols, col_types = get_columns_and_types(config)

    sql = [
        f"{col} {col_type}"
        for (col, col_type) in zip(cols, col_types)
        if col_types != "MANY"
    ]

    sql = ", ".join(sql)
    sql = f"CREATE TABLE main({sql})"
    cur.execute(sql)

    sql = "CREATE TABLE depend_on(commit_hash text)"
    cur.execute(sql)

    for col, col_type in zip(cols, col_types):
        if col_type == "MANY":
            cur.execute(f"CREATE TABLE {col}(id TEXT, value TEXT)")

    con.commit()
    con.close()

    return cols


def construct_sql_logs(file_add, file_remove, config, name, state):
    """Construct sql logs from the file add and file remove

    # Args
        file_add <[(str, str)]>: file name and file hash to add
        file_remove <[(str, str)]>: file name and file hash to remove

    # Returns
        <[str]>: sql statements

    # @TODO: currently it assumes that the ID exists
    """
    pattern = re.compile(config[name]['PATTERN'])
    conversion_groups = get_group_rule(config[name])
    path_cols = get_path_cols(config[name])
    primary_cols = get_primary_cols(config[name])

    logic = defaultdict(dict)
    for fn, fh in file_remove:
        if fn == '.godconfig.yml':
            continue
        match = pattern.match(fn)
        if match is None:
            continue

        match_dict = match.groupdict()

        # get the id
        if 'id' not in match_dict:
            continue

        id_ = match_dict.pop('id')
        for group, match_key in match_dict.items():
            if group in conversion_groups:
                match_value = conversion_groups[group][match_key]

                items = logic[id_].get(match_value, [])
                items.append(('-', fn))
                logic[id_][match_value] = items

                items = logic[id_].get(match_value + '_hash', [])
                items.append(('-', fh))
                logic[id_][match_value + '_hash'] = items

            else:
                if group in path_cols:
                    items = logic[id_].get(group, [])
                    items.append(('-', match_key))
                    logic[id_][group] = items

                    items = logic[id_].get(group + '_hash', [])
                    items.append(('-', fh))
                    logic[id_][group + '_hash'] = items
                else:
                    items = logic[id_].get(group, [])
                    items.append(('-', match_key))
                    logic[id_][group] = items

    for fn, fh in file_add:
        if fn == '.godconfig.yml':
            continue
        match = pattern.match(fn)
        if match is None:
            continue

        match_dict = match.groupdict()

        # get the id
        if 'id' not in match_dict:
            continue

        id_ = match_dict.pop('id')
        for group, match_key in match_dict.items():
            if group in conversion_groups:
                match_value = conversion_groups[group][match_key]

                items = logic[id_].get(match_value, [])
                items.append(('+', fn))
                logic[id_][match_value] = items

                items = logic[id_].get(match_value + '_hash', [])
                items.append(('+', fh))
                logic[id_][match_value + '_hash'] = items

            else:
                if group in path_cols:
                    items = logic[id_].get(group, [])
                    items.append(('+', match_key))
                    logic[id_][group] = items

                    items = logic[id_].get(group + '_hash', [])
                    items.append(('+', fh))
                    logic[id_][group + '_hash'] = items
                else:
                    items = logic[id_].get(group, [])
                    items.append(('+', match_key))
                    logic[id_][group] = items

    # construct db to compare
    index_db_path = Path(settings.DIR_INDEX, name)
    if index_db_path.exists():
        commit = get_db_commit(str(index_db_path))
        db = load_index_db(str(index_db_path))
    else:
        commit = None
        db = {}

    # sql logic
    sql_statements = []
    for fid, cols in logic.items():
        if fid in db:
            sql_statement = []
            drop = False
            for col_name, changes in cols.items():
                op, value = changes[-1]
                if op == '+' and value != db[fid][col_name]:
                    sql_statement.append(f'{col_name} = "{value}"')
                elif op == '-':
                    if col_name in primary_cols:
                        drop = True
                    sql_statement.append(f'{col_name} = NULL')
            if drop:
                sql_statements.append(f'DELETE FROM main WHERE ID="{fid}"')
                continue

            if sql_statement:
                sql_statements.append(
                    f"UPDATE main SET {', '.join(sql_statement)} WHERE ID = \"{fid}\"")
        else:
            add_col, add_val = [], []
            for col_name, changes in cols.items():
                op, value = changes[-1]
                if op == '+':
                    add_col.append(col_name)
                    add_val.append(f'{value}')
            if add_col:
                add_col = ["ID"] + add_col
                add_val = [fid] + add_val
                sql_statements.append(
                    f"INSERT INTO main {tuple(add_col)} VALUES {tuple(add_val)}")

    # update the database
    if not index_db_path.exists():
        create_index_db(config[name], name)

    commit_hash = get_db_commit(str(index_db_path))
    con = sqlite3.connect(str(index_db_path))
    cur = con.cursor()
    for sql_statement in sql_statements:
        cur.execute(sql_statement)
    con.commit()
    if commit_hash:
        cur.execute("DELETE FROM depend_on")
    cur.execute(f'INSERT INTO depend_on (commit_hash) VALUES ("{state}")')
    con.commit()

    con.close()

    # save the logs

    return sql_statements


def populate_db_from_sql_logs(sql_logs):

    con = sqlite3.connect(str(Path(settings.DIR_INDEX, NAME)))
    cur = con.cursor()

    for each_statement in sql_logs:
        cur.execute(each_statement)

    con.commit()
    con.close()


if __name__ == "__main__":
    # sql_logs = construct_sql_logs()
    # result = create_db(TYPE2)

    # file_add, file_remove = get_transform_operations(
    #     # "477ea9463b74aa740be85359ed69a1ab90f0b545bcc238d629b6bb76803e700d",
    #     # "4edd28d87b4223f086c6bb44c838082456eb7b5f97892311e5612aeb84fb9573",
    #     "44ba89e3f3afa22482b4961b4480371b38d521b8cb1c08c350f763375c915a47",
    #     "ff7a72be8907be6dc50901db67baf268b1a784d8817a0021dbbaa8ca79cd362c",
    #     # "11a7936355d055bc5437d9fc7f22926ee91fced3f947491d41655fac041d6e23",
    #     # "331cc680329fdef08c5b030c651de2b624f864e16744a476078fd02fde820dfa"
    # )
    file_add, file_remove = get_transform_operations(
            "001b32f966fea54404e0370c7f3f28933cb251e5f98463eecfb1e920d8fb7cea")
    result = construct_sql_logs(file_add, file_remove, TYPE4)
    # populate_db_from_sql_logs(sql_logs)
