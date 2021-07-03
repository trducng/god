import re
from collections import defaultdict

from god.records.configs import (
    get_path_cols,
    get_group_rule,
)


class RecordLogs:
    pass


def parse_transformation(transform, op, result_dict, config):
    """Parse transformation

    # @TODO: it seems logs and configs are DB-agnostic functions, we can move these
    # scripts into default locations. Rename this module from sqlrecords -> records

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


def construct_transformation_logs(file_add, file_remove, config):
    """Construct sql logs from the file add and file remove

    # Args
        file_add <[(str, str)]>: file name and file hash to add
        file_remove <[(str, str)]>: file name and file hash to remove
        config <Settings>: the record config

    # Returns
        <[str]>: sql statements
    """
    logic = defaultdict(dict)  # {id: {col: [(-/+, val)]}}
    logic = parse_transformation(file_remove, "-", logic, config)
    logic = parse_transformation(file_add, "+", logic, config)

    return logic
