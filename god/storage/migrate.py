from god.storage.commons import BaseStorage, LocalStorage


def get_files():
    pass


def set_files():
    pass


def migrate_local_storage(storage1: BaseStorage, storage2: BaseStorage):
    """Migrate relating to local storage"""
    from pathlib import Path

    if isinstance(storage1, LocalStorage):

        # migrate objects
        object_paths = storage1.list_objects()
        paths, hash_values = [], []
        for each in object_paths:
            paths.append(str(storage1._object_path / each))
            hash_values.append("".join(Path(each).parts))
        storage2.store_objects(paths, hash_values)

        # migrate dirs
        dir_paths = storage1.list_dirs()
        paths, hash_values = [], []
        for each in dir_paths:
            paths.append(str(storage1._dir_path / each))
            hash_values.append("".join(Path(each).parts))
        storage2.store_dirs(paths, hash_values)

        # migrate commits
        commit_paths = storage1.list_commits()
        paths, hash_values = [], []
        for each in commit_paths:
            paths.append(str(storage1._commit_path / each))
            hash_values.append("".join(Path(each).parts))
        storage2.store_commits(paths, hash_values)
        return


def safe_migrate(storage1: BaseStorage, storage2: BaseStorage):
    """Migrate data from `storage1` to `storage2`

    In essence, this method:
        1. Downloads objects from `storage1` to local.
        2. Then uploads those objects from local to `storage2`.
    """
    if isinstance(storage1, LocalStorage) or isinstance(storage2, LocalStorage):
        migrate_local_storage(storage1, storage2)
        return

    for item in storage1.get_hashes():
        # download
        # upload
        storage2.store_files()  # PRIORITY0: continue from here, upload directly


def migrate(storage1: BaseStorage, storage2: BaseStorage):
    """Migrate data from `storage1` to `storage2`

    This stragegy might not be optimal as there can be specific methods to transfer
    data directly from `storage1` to `storage2`, without needing that data in local.
    Since such strategy typically requires providers of both storages to support each
    other, and `god` does not have control of those providers, so we will support
    optimized transfer on best-effort basis. Otherwise, fall back to the safe transfer
    """
    safe_migrate(storage1, storage2)
