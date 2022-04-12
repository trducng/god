import json
from pathlib import Path

from god.core.common import get_base_dir


def load_manifest(name):
    m = Path(get_base_dir(), ".god", "workings", "plugins", "tracks", name)
    if not m.is_file():
        return {}
    with m.open("r") as fi:
        return json.load(fi)["info"]
