import re
import sqlite3
from pathlib import Path
from collections import defaultdict

from constants import BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR, DB_DIR, ORGE_DIR
from logs import get_state_ops, get_transform_operations


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
        "3d_mask": {"path": True, "conversion_group": ("switch_", "/geometry.pbdata")},
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
        if not isinstance(col_rule, dict):
            continue
        if col_rule.get('path', False):
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
        if not isinstance(col_rule, dict):
            continue
        if 'conversion_group' not in col_rule:
            continue
        group_name, group_val = col_rule['conversion_group']
        if isinstance(group_val, str):
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


def create_db(config):
    """Create SQL database from config

    # Args:
        config <dict>: the configuration file
    """
    con = sqlite3.connect(str(Path(ORGE_DIR, config["NAME"])))
    cur = con.cursor()

    pattern = re.compile(config["PATTERN"])
    cols, col_types = get_columns_and_types(config)

    sql = [f"{col} {col_type}" for (col, col_type) in zip(cols, col_types)]
    sql = ", ".join(sql)
    sql = f"CREATE TABLE main({sql})"

    cur.execute(sql)
    con.commit()
    con.close()

    return cols


def construct_sql_logs(file_add, file_remove, config):
    """Construct sql logs from the file add and file remove

    # Args
        file_add <[(str, str)]>: file name and file hash to add
        file_remove <[(str, str)]>: file name and file hash to remove

    # Returns
        <[str]>: sql statements

    # @TODO: currently it assumes that the ID exists
    """
    pattern = re.compile(config['PATTERN'])
    conversion_groups = get_group_rule(config)
    path_cols = get_path_cols(config)

    logic = defaultdict(dict)
    for fn, fh in file_add:
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

    # construct logic

    return logic



def populate_db_from_sql_logs(sql_logs):

    con = sqlite3.connect(str(Path(ORGE_DIR, NAME)))
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
    file_add, file_remove = get_state_ops("001b32f966fea54404e0370c7f3f28933cb251e5f98463eecfb1e920d8fb7cea")
    result = construct_sql_logs(file_add, file_remove, TYPE4)
    import pdb; pdb.set_trace()
    # populate_db_from_sql_logs(sql_logs)
