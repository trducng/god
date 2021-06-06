class RepoExisted(Exception):
    """Repo being initialized already exists"""
    pass

class FileExisted(Exception):
    """Operations that cannot create file if file already exists"""
    pass
