import shutil
from pathlib import Path
from typing import Dict, List

from god.core.common import get_base_dir
from god.storage.backends.base import BaseStorage

DEFAULT_PATH = "objects"
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

    def get_files(self, hash_values: List[str], file_paths: List[str]):
        """Get the file and store in file_path

        Args:
            hash_values: the object hash value
            file_paths: the file path to copy to
        """
        for each_hash, each_path in zip(hash_values, file_paths):
            shutil.copy(self._path / self._get_hash_path(each_hash), each_path)

    def store_files(self, file_paths: List[str], hash_values: List[str]):
        """Store a file with a specific hash value

        Args:
            file_path: the file path
            hash_value: the hash value of the file
        """
        for each_hash, each_path in zip(hash_values, file_paths):
            hash_path = self._path / self._get_hash_path(each_hash)
            if hash_path.exists():
                continue

            hash_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(each_path, hash_path)

    def deletes(self, hash_values: List[str]):
        """Delete object that has specific hash value

        Args:
            hash_value: the hash value of the object
        """
        for each_hash in hash_values:
            hash_path = self._path / self._get_hash_path(each_hash)
            if hash_path.exists():
                hash_path.unlink()

    def exists(self, hash_values: List[str]) -> List[bool]:
        """Check whether an object or a file with a specific hash value exist

        Args:
            hash_value: the file hash value

        Returns:
            True if the file exists, False otherwise
        """
        result = []
        for each_hash in hash_values:
            result.append((self._path / self._get_hash_path(each_hash)).exists())
        return result

    def get_hashes(self) -> List[str]:
        """Return all hashes inside the object storage"""
        return []
