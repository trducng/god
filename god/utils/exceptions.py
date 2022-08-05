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


class InternalNodeNotFound(Exception):
    """Internal node not found"""

    pass


class PluginNotFound(Exception):
    """Plugin not founded"""

    pass
