import re
import sqlite3
from pathlib import Path

from constants import BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR, DB_DIR, ORGE_DIR
from logs import get_state_ops


TYPE1 = {
    "NAME": "index",
    "PATTERN": "train1/(?P<label>cat|dog)\.(?P<id>\d+)\..+$",
    "COLUMNS": {
        "id": "INTEGER",
        "label": "TEXT",
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


TYPE_3_4 = {
    "NAME": "index",
    "PATTERN": "train/(?P<class>.+?)/(?P<id>.+\d+)(?P<switch_>.pbdata|/geometry.pbdata|/video.MOV)$",
    "PATH": {
        "group": "switch_",
        "conversion": {
            ".pbdata": "location",
            "/geometry.pbdata": "3d_mask",
            "/video.MOV": "input",
        },
    },
}


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


def construct_sql_logs():
    items = get_state_ops(".")
    pattern = re.compile(TABLE_DEF["ID"])

    # TODO basically everything is add here
    sql_logs = []
    for each_name, each_hash in items:
        cols = {}
        result = pattern.match(each_name)  # TODO HERE
        if not result:
            continue

        result_dict = result.groupdict()
        if "id" not in result_dict:
            continue

        # TODO: should check if this is a add / edit / remove, for now let assume
        # it is add for simplicity.
        # INSERT INTO main(id, path, hash, label) VALUES("{id}", "{path}", "{hash}", "{label}")

        result_dict["hash"] = each_hash
        result_dict["path"] = each_name
        id_, path = result_dict["id"], result_dict["path"]
        hash_, label = result_dict["hash"], result_dict["label"]

        sql_log = f'INSERT INTO main(id, path, hash, label) VALUES("{id_}", "{path}", "{hash_}", "{label}")'
        sql_logs.append(sql_log)

    return sql_logs


def populate_db_from_sql_logs(sql_logs):

    con = sqlite3.connect(str(Path(ORGE_DIR, NAME)))
    cur = con.cursor()

    for each_statement in sql_logs:
        cur.execute(each_statement)

    con.commit()
    con.close()


if __name__ == "__main__":
    # sql_logs = construct_sql_logs()
    result = create_db(TYPE2)
    import pdb

    pdb.set_trace()
    # populate_db_from_sql_logs(sql_logs)
