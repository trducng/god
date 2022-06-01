COLUMNS = [
    ("name", "text"),
    ("hash", "text"),
    ("mhash", "text"),
    ("remove", "integer"),
    ("mtime", "real"),
    ("exe", "real"),
    ("ignore", "integer"),
    ("conflict", "text"),
]


def column_index(name: str) -> int:
    """Get index of a column

    Args:
        name: column name

    Retuns:
        Index of the column

    Raise:
        IndexError: when `name` is not inside COLUMNS
    """
    for idx, column in enumerate(COLUMNS):
        if column[0] == name:
            return idx

    raise IndexError(
        f"Invalid index column '{name}'. Accept: {list(_[0] for _ in COLUMNS)}"
    )
