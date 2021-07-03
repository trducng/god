from collections import defaultdict

from god.base import Settings


def get_path_cols(config):
    """Get the group rule

    # Args:
        config <{}>: the configuration

    # Returns:
    """
    result = []

    COLUMNS = config.get("COLUMNS", {})
    for col_name, col_rule in COLUMNS.items():
        if not isinstance(col_rule, (dict, Settings)):
            continue
        if col_rule.get("path", False) or col_rule.get("PATH", False):
            result.append(col_name)

    return result


def get_group_rule(config):
    """Get the group rule

    # Args:
        config <{}>: the configuration

    # Returns:
        <{}>: {group_name: {match_value: col_name}}
    """
    result = defaultdict(dict)

    COLUMNS = config.get("COLUMNS", {})
    for col_name, col_rule in COLUMNS.items():
        if not isinstance(col_rule, (dict, Settings)):
            continue
        if "conversion_group" not in col_rule:
            continue

        group_name = list(col_rule["conversion_group"].keys())[0]
        group_val = list(col_rule["conversion_group"].values())[0]
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
