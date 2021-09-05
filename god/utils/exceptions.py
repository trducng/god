class RepoExisted(Exception):
    """Repo being initialized already exists"""

    pass


class FileExisted(Exception):
    """Operations that cannot create file if file already exists"""

    pass


class InvalidUserParams(Exception):
    """Invalid supplied params"""

    pass


class OperationNotPermitted(Exception):
    """Operation not permitted in the repo"""

    pass


class RecordEntryNotFound(Exception):
    """Record entry not found"""

    pass


class InternalNodeNotFound(Exception):
    """Internal node not found"""

    pass


class RecordParsingError(Exception):
    """Happens when there is problem with parsing records"""

    pass


class RecordNotExisted(Exception):
    """Record defined in config but not showing up in index"""

    pass
