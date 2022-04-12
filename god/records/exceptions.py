class RecordEntryNotFound(Exception):
    """Record entry not found"""

    pass


class RecordParsingError(Exception):
    """Happens when there is problem with parsing records"""

    pass


class RecordNotExisted(Exception):
    """Record defined in config but not showing up in index"""

    pass
