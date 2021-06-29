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



def construct_sql_logs(file_add, file_remove, config, name, state):
    """Construct sql logs from the file add and file remove

    # Args
        file_add <[(str, str)]>: file name and file hash to add
        file_remove <[(str, str)]>: file name and file hash to remove

    # Returns
        <[str]>: sql statements

    # @TODO: currently it assumes that the ID exists
    """
    # construct db to compare
    index_db_path = Path(settings.DIR_INDEX, name)
    if index_db_path.exists():
        commit = get_db_commit(str(index_db_path))
        db = load_index_db(str(index_db_path))
    else:
        commit = None
        db = {}


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
