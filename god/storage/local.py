import shutil
from pathlib import Path
from typing import Dict, List

import click

from god.core.common import get_base_dir
from god.storage.base import BaseStorage

DEFAULT_PATH = "storage"
DEFAULT_DIR_LEVEL = 2


class LocalStorage(BaseStorage):
    """Store objects locally"""

    def __init__(self, config: Dict):
        # TODO: decide the format for storage config
        # TODO: might only allow relative path (to avoid overwrite hacking)
        self._path = Path(
            config.get("PATH", Path(get_base_dir(), ".god", DEFAULT_PATH))
        )
        self._dir_levels = config.get("DIR_LEVEL", DEFAULT_DIR_LEVEL)

    def _get_hash_path(self, hash_value: str) -> str:
        """From hash value, get relative hash path"""
        components = [
            hash_value[idx * 2 : (idx + 1) * 2] for idx in range(self._dir_levels)
        ]
        return str(Path(*components, hash_value[self._dir_levels * 2 :]))

    def get_file(self, hash_value: str, file_path: str):
        """Get the file and store in file_path

        Args:
            hash_value: the object hash value
            file_path: the file path to copy to
        """
        shutil.copy(self._path / self._get_hash_path(hash_value), file_path)

    def get_object(self, hash_value: str) -> bytes:
        """Get the file and store in file_path

        Args:
            hash_value: the object hash value

        Returns:
            the object bytes
        """
        with (self._path / self._get_hash_path(hash_value)).open("rb") as fi:
            b = fi.read()
        return b

    def store_file(self, file_path: str, hash_value: str):
        """Store a file with a specific hash value

        Args:
            file_path: the file path
            hash_value: the hash value of the file
        """
        hash_path = self._path / self._get_hash_path(hash_value)
        hash_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(file_path, hash_path)
        hash_path.chmod(0o440)

    def store_object(self, obj: bytes, hash_value: str):
        """Store an object with a specific hash value

        Args:
            obj: the object to store
            file_path: the file path
        """
        with (self._path / self._get_hash_path(hash_value)).open("wb") as fo:
            fo.write(obj)

    def delete(self, hash_value: str):
        """Delete object that has specific hash value

        Args:
            hash_value: the hash value of the object
        """
        hash_path = self._path / self._get_hash_path(hash_value)
        if hash_path.exists():
            hash_path.unlink()

    def exists(self, hash_value: str) -> bool:
        """Check whether an object or a file with a specific hash value exist

        Args:
            hash_value: the file hash value

        Returns:
            True if the file exists, False otherwise
        """
        return (self._path / self._get_hash_path(hash_value)).exists()

    def get_hashes(self) -> List[str]:
        """Return all hashes inside the object storage"""
        return []


ls = LocalStorage({})


@click.group()
def main():
    """Local storage manager"""
    pass


@main.command("store-file")
@click.argument("file-path", type=str)
@click.argument("file-hash", type=str)
def store_file(file_path, file_hash):
    ls.store_file(file_path, file_hash)
