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
