import shutil
from pathlib import Path
from typing import List

import god.storage.constants as c
from god.core.common import get_base_dir
from god.storage.backends.base import BaseStorage

DEFAULT_DIR_LEVEL = 2


def parse_config(config: str) -> Path:
    if config.startswith("file://"):
        config = config[7:]
    else:
        raise ValueError(f'Expect "file://" but receive {config} instead')

    if not config:
        return Path(get_base_dir(), ".god")

    path = Path(config)
    if not path.is_absolute():
        raise ValueError("Expect absoute file path (e.g. file:///path/to/file")

    return path.resolve()


class LocalStorage(BaseStorage):
    """Store objects locally"""

    def __init__(self, config: str):
        # TODO: decide the format for storage config
        # TODO: might only allow relative path (to avoid overwrite hacking)
        self._base_path = parse_config(config)
        self._object_path = self._base_path / c.DIR_OBJECTS
        self._dir_path = self._base_path / c.DIR_DIRS
        self._commit_path = self._base_path / c.DIR_COMMITS
        self._dir_levels = DEFAULT_DIR_LEVEL

    def _get_hash_path(self, hash_value: str) -> str:
        """From hash value, get relative hash path"""
        components = [
            hash_value[idx * 2 : (idx + 1) * 2] for idx in range(self._dir_levels)
        ]
        return str(Path(*components, hash_value[self._dir_levels * 2 :]))

    def _get(self, path: Path, hash_values: List[str], file_paths: List[str]):
        """Get the file and store in file_path

        Args:
            hash_values: the object hash value
            file_paths: the file path to copy to
        """
        for each_hash, each_path in zip(hash_values, file_paths):
            shutil.copy(path / self._get_hash_path(each_hash), each_path)

    def _store(self, path: Path, file_paths: List[str], hash_values: List[str]):
        """Store a file with a specific hash value

        Args:
            file_path: the file path
            hash_value: the hash value of the file
        """
        for each_hash, each_path in zip(hash_values, file_paths):
            hash_path = path / self._get_hash_path(each_hash)
            if hash_path.exists():
                continue

            hash_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(each_path, hash_path)

    def _delete(self, path: Path, hash_values: List[str]):
        """Delete object that has specific hash value

        Args:
            hash_value: the hash value of the object
        """
        for each_hash in hash_values:
            hash_path = path / self._get_hash_path(each_hash)
            if hash_path.exists():
                hash_path.unlink()

    def _have(self, path: Path, hash_values: List[str]) -> List[bool]:
        """Check whether an object or a file with a specific hash value exist

        Args:
            hash_value: the file hash value

        Returns:
            True if the file exists, False otherwise
        """
        result = []
        for each_hash in hash_values:
            result.append((path / self._get_hash_path(each_hash)).exists())
        return result

    def _list(self, path: Path) -> List[str]:
        """Return all hashes and location inside the object storage"""
        path = path.resolve()

        result = []
        for item in path.glob("**/????*"):
            if item.is_file:
                result.append(str(item.relative_to(path)))

        return result

    ### objects
    def get_objects(self, hash_values: List[str], paths: List[str]):
        return self._get(self._object_path, hash_values, paths)

    def store_objects(self, paths: List[str], hash_values: List[str]):
        return self._store(self._object_path, paths, hash_values)

    def delete_objects(self, hash_values: List[str]):
        return self._delete(self._object_path, hash_values)

    def have_objects(self, hash_values: List[str]) -> List[bool]:
        return self._have(self._object_path, hash_values)

    def list_objects(self) -> List[str]:
        return self._list(self._object_path)

    ### dirs
    def get_dirs(self, hash_values: List[str], paths: List[str]):
        return self._get(self._dir_path, hash_values, paths)

    def store_dirs(self, paths: List[str], hash_values: List[str]):
        return self._store(self._dir_path, paths, hash_values)

    def delete_dirs(self, hash_values: List[str]):
        return self._delete(self._dir_path, hash_values)

    def have_dirs(self, hash_values: List[str]) -> List[bool]:
        return self._have(self._dir_path, hash_values)

    def list_dirs(self) -> List[str]:
        return self._list(self._dir_path)

    ### commits
    def get_commits(self, hash_values: List[str], paths: List[str]):
        return self._get(self._commit_path, hash_values, paths)

    def store_commits(self, paths: List[str], hash_values: List[str]):
        return self._store(self._commit_path, paths, hash_values)

    def delete_commits(self, hash_values: List[str]):
        return self._delete(self._commit_path, hash_values)

    def comits_exist(self, hash_values: List[str]) -> List[bool]:
        return self._have(self._commit_path, hash_values)

    def have_commits(self) -> List[str]:
        return self._list(self._commit_path)
