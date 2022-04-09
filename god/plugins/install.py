import shutil
from pathlib import Path

from god.core.common import get_base_dir
from god.utils.constants import DIR_INDICES, DIR_HIDDEN_WORKING
from god.index.base import Index


def construct_working_directory(name: str):
    working_dir = Path(get_base_dir(), DIR_HIDDEN_WORKING, name , "tracks")
    working_dir.mkdir(exist_ok=True, parents=True)
    return working_dir.parent


def create_blank_index(name):
    index_path = Path(get_base_dir(), DIR_INDICES, name)
    Index(index_path).build()
    return index_path
