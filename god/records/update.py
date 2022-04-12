from pathlib import Path
from typing import List

from god.records.configs import RecordsConfig
from god.records.constants import RECORDS_INTERNALS, RECORDS_LEAVES, RECORDS_ROOT
from god.records.storage import prolly_edit, prolly_locate

COL_TYPE_INT = "INTEGER"
COL_TYPE_FLOAT = "FLOAT"
COL_TYPE_STR = "TEXT"
COL_TYPE_ARRAY_INT = "ARRAY[INTEGER]"
COL_TYPE_ARRAY_FLOAT = "ARRAY[FLOAT]"
COL_TYPE_ARRAY_TEXT = "ARRAY[TEXT]"


def parse_update_rule(rule: str, cols_types: dict) -> tuple:
    """Parse update rule

    Args:
        rule: user supplied column update rule
        cols_types: column type
    """
    stop_sep = rule.index("=")
    start_sep = stop_sep - 1 if rule[stop_sep - 1] in ["+", "-"] else stop_sep

    col = rule[:start_sep]
    op = rule[start_sep : stop_sep + 1]
    value = rule[stop_sep + 1 :]

    col_type = None
    if col not in cols_types:
        raise AttributeError(f"unknown column {col} in rule {rule}")
    col_type = cols_types[col]

    if op in ["+=", "-="] and col_type not in [
        COL_TYPE_ARRAY_INT,
        COL_TYPE_ARRAY_FLOAT,
        COL_TYPE_ARRAY_TEXT,
    ]:
        raise AttributeError(
            f"{op} can only be used with array type, col {col} has type {col_type}"
        )

    if col_type == COL_TYPE_INT:
        value = int(value)
    elif col_type == COL_TYPE_FLOAT:
        value = float(value)
    elif col_type == COL_TYPE_ARRAY_INT:
        value = [int(value)]
    elif col_type == COL_TYPE_ARRAY_FLOAT:
        value = [float(value)]
    elif col_type == COL_TYPE_ARRAY_TEXT:
        value = [value]
    elif col_type == COL_TYPE_STR:
        value = value
    else:
        raise AttributeError(f"unknown column type {col_type}")

    return col, op, value


def update(
    ids: List, sets: List, dels: List, config: RecordsConfig, dir_records: str
) -> None:
    """Update entries in the records

    Example:
        $ god records update <index> <file-pattern> \
                --set "col1=val1" --set "col2+=val2" --set "col3-=val3"
        $ god records update <index> <file-pattern> \
                --del col1 --del col2

    This operation:
        1. Checks which files to add to records
        2. Checks which files to update in records
        3. Updates the records, get the hash
        4. Updates whash in index_path

    Args:
        ids: the instance ids in record
        sets: each item is a string with format col=val, col+=val, col-=val
        dels: each item is a string col name
        config: the record config
        dir_records: place to store records
    """
    with Path(dir_records, RECORDS_ROOT).open("r") as fi:
        root = fi.read().strip()

    items = prolly_locate(
        keys=ids,
        root=root,
        tree_dir=str(Path(dir_records, RECORDS_INTERNALS)),
        leaf_dir=str(Path(dir_records, RECORDS_LEAVES)),
    )

    add = {}
    update = {}

    cols_types = {
        col: col_type for col, col_type in zip(*config.get_columns_and_types())
    }
    non_auto_cols = config.get_nonauto_columns()
    vals = {}
    for rule in sets:
        col, op, val = parse_update_rule(rule, cols_types)
        if col not in non_auto_cols:
            raise AttributeError(f"cannot change values for col {col}")
        vals[col] = (op, val)

    for id_, instance in items.items():
        if instance is None:
            value = {col: value[1] for col, value in vals.items() if value[0] != "-="}
            add[id_] = value
            continue

        value = {}
        for col, (op, val) in vals.items():
            col_type = cols_types[col]
            if col_type not in [
                COL_TYPE_ARRAY_INT,
                COL_TYPE_ARRAY_FLOAT,
                COL_TYPE_ARRAY_TEXT,
            ]:
                base_val = instance.get(col, [])
                if op == "=":
                    base_val = val
                elif op == "+=":
                    base_val = sorted(list(set(base_val).union(set(val))))
                elif op == "-=":
                    base_val = sorted(list(set(base_val).difference(set(val))))
                value[col] = base_val
            else:
                value[col] = val
        for col in dels:
            value[col] = None
        update[id_] = value

    new_root = prolly_edit(
        root=root,
        tree_dir=str(Path(dir_records, RECORDS_INTERNALS)),
        leaf_dir=str(Path(dir_records, RECORDS_LEAVES)),
        insert=add,
        update=update,
    )

    with Path(dir_records, RECORDS_ROOT).open("w") as fo:
        fo.write(new_root)
