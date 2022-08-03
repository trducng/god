import shutil
from pathlib import Path
from typing import Callable, List, Union

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
        raise ValueError("Expect absoute file path (e.g. file:///path/to/file)")

    return path.resolve()


class LocalStorage(BaseStorage):
    """Store objects locally"""

    def __init__(self, config: str):
        # TODO: decide the format for storage config
        # TODO: might only allow relative path (to avoid overwrite hacking)
        self._base_path = parse_config(config)
        self._dir_levels = DEFAULT_DIR_LEVEL

    def _hash_path(self, hash_value: str, prefix: str = "") -> str:
        """From hash value, get relative hash path"""
        components = [
            hash_value[idx * 2 : (idx + 1) * 2] for idx in range(self._dir_levels)
        ]
        return str(
            Path(
                self._base_path, prefix, *components, hash_value[self._dir_levels * 2 :]
            )
        )

    def _get(
        self,
        storage_paths: List[str],
        paths: List[str],
        progress_callback: Union[Callable, None] = None,
        n_processes: Union[int, None] = None,
    ):
        """Get the file and store in file_path

        Ignore `n_processes` as copying multiple files at once within local computer
        can be slower than copying files sequentially (HDD disk seek).

        Args:
            storage_paths: the path from storage
            paths: the file path to copy to
            progress_callback: it is passed total_files (int) and total_bytes (int)
            n_processes: number of processes to handle getting objects
        """
        for idx, (storage_path, path) in enumerate(zip(storage_paths, paths)):
            parent = Path(path).parent
            parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(storage_path, path)
            if progress_callback:
                progress_callback(total_files=idx + 1, total_bytes=0)

    def _store(self, storage_paths: List[str], paths: List[str]):
        """Store a file with a specific hash value

        Args:
            storage_paths: the path from storage
            paths: the file path to send to storage
        """
        for storage_path, path in zip(storage_paths, paths):
            storage_path = Path(storage_path)
            if storage_path.exists():
                continue

            storage_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(path, storage_path)

    def _delete(self, storage_paths: List[str]):
        """Delete object

        Args:
            storage_paths: the location of object to delete
        """
        for storage_path in storage_paths:
            storage_path = Path(storage_path)
            if storage_path.exists():
                storage_path.unlink()

    def _have(self, storage_paths: List[str]) -> List[bool]:
        """Check whether a file with specific location exists in the storage

        Args:
            storage_paths: the location of the file

        Returns:
            True if the file exists, False otherwise
        """
        result = []
        for storage_path in storage_paths:
            result.append(Path(storage_path).exists())
        return result

    def _list(self, storage_prefix: str) -> List[str]:
        """Return all hashes and location inside the object storage"""
        path = Path(storage_prefix).resolve()

        result = []
        for item in path.glob("**/????*"):
            if item.is_file:
                result.append(str(item.relative_to(path)).replace("/", ""))

        return result
