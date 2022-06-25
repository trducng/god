from pathlib import Path
from typing import Union

from god.core.common import get_base_dir
from god.index.base import Index
from god.utils.constants import DIR_HIDDEN_WORKING, DIR_INDICES


def construct_working_directory(name: str, path: Union[str, Path] = None):
    working_dir = Path(get_base_dir(path), DIR_HIDDEN_WORKING, name, "tracks")
    working_dir.mkdir(exist_ok=True, parents=True)
    working_dir = Path(get_base_dir(path), DIR_HIDDEN_WORKING, name, "untracks")
    working_dir.mkdir(exist_ok=True, parents=True)

    return working_dir.parent


def create_blank_index(name, path: Union[str, Path] = None):
    index_path = Path(get_base_dir(path), DIR_INDICES, name)
    Index(index_path).build()
    return index_path
