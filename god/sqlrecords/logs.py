import re
from collections import defaultdict

from god.sqlrecords.configs import (
    get_path_cols,
    get_group_rule,
    get_columns_and_types,
    get_primary_cols,
)


class RecordLogs:
    pass


def parse_transformation(transform, op, result_dict, config):
    """Parse transformation

    # Args:
        transform <{}>: filepath: filehash
        op <str>: whether '+' (add) or '-' (remove)
        result_dict <{str: {str: [('+/-', str)]}}>: place to store result
        config <Settings>: the record config

    # Returns:
        <{str: {str: [('+/-', str)]}}>: the result (stored in `result_dict`)
    """
    ALLOWED_OPS = ["+", "-"]
    if op not in ALLOWED_OPS:
        raise AttributeError(f"Unknown op, should be in {ALLOWED_OPS} but {op}")

    pattern = re.compile(config["PATTERN"])
    conversion_groups = get_group_rule(config)
    path_cols = set(get_path_cols(config))
    primary_cols = set(get_primary_cols(config))

    result_dict = defaultdict(dict)  # {id: {col: [(-/+, val)]}}
    for fn, fh in transform.items():
        match = pattern.match(fn)
        if match is None:
            continue

        match_dict = match.groupdict()

        # get the id
        if "id" not in match_dict:
            continue

        id_ = match_dict.pop("id")
        for group, match_key in match_dict.items():
            if group in conversion_groups:
                match_value = conversion_groups[group][match_key]

                items = result_dict[id_].get(match_value, [])
                items.append((op, fn))
                result_dict[id_][match_value] = items

                items = result_dict[id_].get(match_value + "_hash", [])
                items.append((op, fh))
                result_dict[id_][match_value + "_hash"] = items

            else:
                if group in path_cols:
                    items = result_dict[id_].get(group, [])
                    items.append((op, match_key))
                    result_dict[id_][group] = items

                    items = result_dict[id_].get(group + "_hash", [])
                    items.append((op, fh))
                    result_dict[id_][group + "_hash"] = items
                else:
                    items = result_dict[id_].get(group, [])
                    items.append((op, match_key))
                    result_dict[id_][group] = items

    return result_dict


def construct_transformation_logs(file_add, file_remove, record_entries, config):
    """Construct sql logs from the file add and file remove

    # Args
        file_add <[(str, str)]>: file name and file hash to add
        file_remove <[(str, str)]>: file name and file hash to remove
        db_entries <{id: {cols: values}}>: entries in current record
        config <Settings>: the record config

    # Returns
        <[str]>: sql statements

    # @TODO: currently it assumes that the ID exists
    """
    logic = defaultdict(dict)  # {id: {col: [(-/+, val)]}}
    logic = parse_transformation(file_remove, "-", logic, config)
    logic = parse_transformation(file_add, "+", logic, config)

    # sql logic
    sql_statements = []
    for fid, cols in logic.items():
        if fid in record_entries:
            sql_statement = []
            drop = False
            for col_name, changes in cols.items():
                op, value = changes[-1]
                if op == "+" and value != record_entries[fid][col_name]:
                    sql_statement.append(f'{col_name} = "{value}"')
                elif op == "-":
                    if col_name in primary_cols:
                        drop = True
                    sql_statement.append(f"{col_name} = NULL")
            if drop:
                sql_statements.append(f'DELETE FROM main WHERE id="{fid}"')
                continue

            if sql_statement:
                sql_statements.append(
                    f"UPDATE main SET {', '.join(sql_statement)} WHERE id = \"{fid}\""
                )
        else:
            add_col, add_val = [], []
            for col_name, changes in cols.items():
                op, value = changes[-1]
                if op == "+":
                    add_col.append(col_name)
                    add_val.append(f"{value}")

            if add_col:
                add_col = ["id"] + add_col
                add_val = [fid] + add_val
                sql_statements.append(
                    f"INSERT INTO main {tuple(add_col)} VALUES {tuple(add_val)}"
                )

    return sql_statements
