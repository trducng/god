def get_columns_and_types_old(config):
    """Get columns and column types from config

    # Args
        config <dict>: orge configuration file

    # Returns
        <[str]>: list of column names
        <[str]>: list of column types
    """
    pattern = re.compile(config["PATTERN"])
    cols, col_types = [], []

    # get columns from PATTERN, ignoring group ends with _
    for each_group in pattern.groupindex.keys():
        if not each_group:  # unnamed group
            continue

        if each_group[-1] == '_':   # group ends with '_'
            continue

        cols.append(each_group)

    cols = [each for each in groups if each[-1] != "_"]

    # get columns from PATH
    if isinstance(config["PATH"], str):
        cols += [config["PATH"], config["PATH"] + "_hash"]
    else:
        path_cols = list(config["PATH"]["conversion"].values())
        for _ in path_cols:
            cols += [_, _ + "_hash"]

    # get columns from EXTRA_COLUMNS
    if config["EXTRA_COLUMNS"]:
        cols += list(config["EXTRA_COLUMNS"].keys())

    return cols, col_types
