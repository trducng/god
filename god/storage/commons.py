import json
from pathlib import Path

from god.core.common import get_base_dir
from god.storage.backends.base import BaseStorage
from god.storage.backends.local import LocalStorage
from god.storage.backends.s3 import S3Storage
from god.utils.constants import FILE_LINK

STORAGE = {
    "file": LocalStorage,
    "s3": S3Storage,
}


def get_backend(path: str = None) -> BaseStorage:
    """Get corresponding backend"""
    if path is None:
        with Path(get_base_dir(), FILE_LINK).open("r") as fi:
            path = json.load(fi)["STORAGE"]
    mode = path.split("://")[0]  # type: ignore

    return STORAGE[mode](path)
