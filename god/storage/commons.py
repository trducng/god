import yaml

from god.remote import get_remote_declaration_config_path
from god.storage.backends.base import BaseStorage
from god.storage.backends.local import LocalStorage
from god.storage.backends.s3 import S3Storage

STORAGE = {
    "file": LocalStorage,
    "s3": S3Storage,
}


def get_backend(path: str = None, base_dir: str = None) -> BaseStorage:
    """Get corresponding backend"""
    if path is None:
        with open(get_remote_declaration_config_path(base_dir=base_dir), "r") as fi:
            path = yaml.safe_load(fi)["storage"]
    mode = path.split("://")[0]  # type: ignore

    return STORAGE[mode](path)
