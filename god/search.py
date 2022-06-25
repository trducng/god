import sqlite3
from pathlib import Path

from god.configs.base import Settings, settings


def get_standard_index(config):
    """Get standard index from config

    # Args
        config <{}> the index config
    """

    if len(config) == 1:
        # if there's only 1 kind of index table, retrieve it
        return list(config.keys())[0]

    for key, value in config.items():
        if value.get("STANDARD", None):
            return key

    # else return the first index in config
    return list(config.keys())[0]


def get_path_cols(config):
    """Get the group rule

    # Args
        config <{}>: the configuration

    # Returns
    """
    result = []

    COLUMNS = config.get("COLUMNS", {})
    for col_name, col_rule in COLUMNS.items():
        if not isinstance(col_rule, (dict, Settings)):
            continue
        if col_rule.get("path", False) or col_rule.get("PATH", False):
            result.append(col_name)

    return result


def get_standard_column(index_config):
    """Get standard column

    # Args
        index_config <{}>: the config of particular index

    # Returns
        [<str>]: list of standard columns
    """
    cols = []
    for key, value in index_config["COLUMNS"].items():
        if isinstance(value, str):
            continue
        if value.get("standard", None):
            cols.append(key)

    return cols


def search_raw_query(db_name, query):
    """Search using raw SQL query

    # Args
        db_name <str>: the name of index database
        query <str>: the raw query to search

    # Returns
        <[[str]]>: the list of result, in list form
    """
    result = []

    con = sqlite3.connect(str(Path(settings.DIR_INDEX, db_name)))
    cur = con.cursor()
    query_result = cur.execute(query)
    result.append(tuple([each[0] for each in query_result.description]))
    result += query_result.fetchall()

    con.close()

    return result


def separate_main_feature_cols(config, **kwargs):
    """Separate main and feature columns

    # Args
        config <{}>: the index configuration
        **kwargs <{}>: the column: query

    # Returns
        <{}>: the column: query for main
        <{}>: the column: query for features
    """
    feature_cols = []
    for key, value in config["COLUMNS"].items():
        if isinstance(value, str):
            continue
        if value.get("type", None) == "MANY":
            feature_cols.append(key)
    feature_cols = set(feature_cols)

    main_queries, feature_queries = {}, {}
    for key, value in kwargs.items():
        if key in feature_cols:
            feature_queries[key] = value
        else:
            main_queries[key] = value

    return main_queries, feature_queries


def parse_main_conditions(**kwargs):
    """Parse the query from user-supplied keyword arguments

    Example queryes:
        col="human||bike%"

    # Arg
        **kwargs <{}>: user-suplied keyword arguments

    # Returns
        <str>: sql query
    """
    query_components = []
    for key, value in kwargs.items():
        temp_queries = value.split("||")
        queries = []
        for temp_query in temp_queries:
            if "*" in temp_query:
                queries.append(f"main.{key} LIKE '{temp_query.replace('*', '%')}'")
            else:
                queries.append(f"main.{key} = '{temp_query}'")

        query = " OR ".join(queries)
        if len(queries) > 1:
            query_components.append(f"({query})")
        else:
            query_components.append(query)

    return " AND ".join(query_components)


def parse_feature_conditions(**kwargs):
    """Parse the query from user-supplied keyword arguments

    Example queryes:
        col="human||bike%"

    # Arg
        **kwargs <{}>: user-suplied keyword arguments

    # Returns
        <str>: sql query
    """
    query_components = []
    for key, value in kwargs.items():
        temp_queries = value.split("||")
        queries = []
        for temp_query in temp_queries:
            if "*" in temp_query:
                queries.append(f"{key}.value LIKE '{temp_query.replace('*', '%')}'")
            else:
                queries.append(f"{key}.value = '{temp_query}'")

        query = " OR ".join(queries)
        if len(queries) > 1:
            query_components.append(f"({query})")
        else:
            query_components.append(query)

    return " AND ".join(query_components)


def search(config, index=None, columns=None, **kwargs):
    """Retrieve instances in index that match with query in **kwargs {col: query}

    # Args
        config <{}>: the index configuration
        index <str>: the index name to search, otherwise the default one in config
        columns <[str]>: the columns to retrieve, otherwise the default one in config
        **kwargs <{}>: the argument to retrieve file ("col": "value")

    # Returns
        <[[str]]>: the list of result, in list form
    """
    if index is None:
        index = get_standard_index(config)

    if columns is None:
        columns = get_standard_column(config[index])

    if columns:
        temp_columns = []
        path_cols = get_path_cols(config[index])
        for column in columns:
            temp_columns.append(f"main.{column}")
            if column in path_cols:
                temp_columns.append(f"main.{column}_hash")
        columns = temp_columns

    columns = ", ".join(columns) if columns else "*"

    # investigate queries
    main_queries, feature_queries = separate_main_feature_cols(config[index], **kwargs)

    main_conditions = parse_main_conditions(**main_queries)
    feature_conditions = parse_feature_conditions(**feature_queries)

    if feature_queries:
        joins = []
        for key in feature_queries.keys():
            joins.append(f"INNER JOIN {key} ON main.id = {key}.id")
        joins = " ".join(joins)
        conditions = (
            " AND ".join([main_conditions, feature_conditions])
            if main_conditions
            else feature_conditions
        )
        sql_query = f"SELECT {columns} FROM main {joins} WHERE {conditions}"
    else:
        conditions = main_conditions
        sql_query = f"SELECT {columns} FROM main WHERE {conditions}"

    return search_raw_query(index, sql_query)


if __name__ == "__main__":
    import yaml

    kwargs = {"class": "bike||camera", "col": "abc"}
    with open("/home/john/datasets/god-test/type4/.godconfig", "r") as f_in:
        config = yaml.safe_load(f_in)["INDEX"]

    print(search(config, index=None, columns=None, **kwargs))
