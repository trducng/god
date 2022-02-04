import json
import sys
from hashlib import sha256
from pathlib import Path
from typing import Dict

import click

from god.core.common import get_base_dir

DEFAULT_PATH = "storage"
DEFAULT_DIR_LEVEL = 2


class BaseDescriptor:
    """Base class to interact with the descriptor inside a repository"""

    def __init__(self, config):
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

    def store_descriptor_object(self, obj: Dict) -> str:
        content = json.dumps(obj, sort_keys=True, ensure_ascii=False)
        hash_value = sha256(content.encode()).hexdigest()
        with (self._path / self._get_hash_path(hash_value)).open("w") as fo:
            fo.write(content)

        return hash_value

    def store_descriptor_file(self, file_path: str) -> str:
        with open(file_path, "r") as fi:
            obj = json.load(fi)

        return self.store_descriptor_object(obj)


descriptor = BaseDescriptor({})


@click.group()
def main():
    """Descriptor manager"""
    pass


@main.command("store-descriptor")
@click.argument("json-string", type=str)
def store_descriptor(json_string: str):
    content = json.loads(json_string)
    fh = descriptor.store_descriptor_object(content)
    print(fh, file=sys.stdout)
