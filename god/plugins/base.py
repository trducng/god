import json
from pathlib import Path
from typing import Dict, List, Union

import god.utils.constants as c
from god.core.common import get_base_dir
from god.core.head import read_HEAD

BUILTIN_PLUGINS = {"records", "snapshots"}


def get_exposed_plugin(base_dir: Union[str, Path, None] = None) -> Union[str, None]:
    """Plugin that is exposed in the base directory

    Args:
        base_dir: the repository path

    Returns:
        The name of the exposed plugin in base directory
    """
    base_dir = Path(get_base_dir(path=base_dir))
    return read_HEAD(base_dir / c.FILE_HEAD).exposed_plugin()


def plugin_endpoints(name: str, base_dir: Union[str, Path] = None) -> Dict[str, str]:
    """Get plugin index-path, track-path, untrack-path, cache-path

    Args:
        name: the name of plugin we wish to know endpoints
        base_dir: the repository path

    Returns:
        [str]: index - index path
        [str]: tracks - track directory
        [str]: untracks - untrack directory
        [str]: cache - cache directory
        [str]: base_dir - the base directory
    """
    base_dir = Path(get_base_dir(path=base_dir))
    result = {
        "index": str(base_dir / c.DIR_INDICES / name),
        "tracks": str(base_dir / c.DIR_HIDDEN_WORKING / name / "tracks"),
        "untracks": str(base_dir / c.DIR_HIDDEN_WORKING / name / "untracks"),
        "cache": str(base_dir / c.DIR_CACHE / name),
        "base_dir": str(base_dir),
    }

    if name == get_exposed_plugin(base_dir):
        result["tracks"] = str(base_dir)

    return result


def installed_plugins(
    plugin_type: int = 0, base_dir: Union[str, Path, None] = None
) -> List[str]:
    """List the name of all installed plugins

    Args:
        plugin_type: can be 0 (all), 1 (active only), 2 (passive only)
        base_dir: the repository path

    Returns:
        List of names of installed plugins
    """
    if plugin_type not in [0, 1, 2]:
        raise AttributeError("plugin_type should be 0 (all), 1 (active), 2 (passive)")

    names = []
    plugin_dir = plugin_endpoints("plugins", base_dir=base_dir)

    manifest_dir = Path(plugin_dir["tracks"], "manifest")
    for each in sorted(manifest_dir.glob("*")):
        names.append(each.name)

    if plugin_type == 0:
        return names

    actives = []
    binary_dir = Path(plugin_dir["untracks"], "bin")
    for each in binary_dir.glob("*"):
        actives.append(each.name.replace("god-", ""))

    if plugin_type == 1:
        return list(sorted(actives))

    return list(sorted(set(names).difference(actives)))


def build_plugin_directories(name: str, base_dir: Union[str, Path, None] = None):
    """Build the plugin tracks and untracks directories

    Args:
        name: the name of plugin we wish to know endpoints
        base_dir: the repository path
    """
    endpoints = plugin_endpoints(name, base_dir)
    Path(endpoints["tracks"]).mkdir(exist_ok=True, parents=True)
    Path(endpoints["untracks"]).mkdir(exist_ok=True, parents=True)


def build_plugin_index(name, base_dir: Union[str, Path, None] = None):
    """Build the index

    Args:
        name: the name of plugin we wish to know endpoints
        base_dir: the repository path
    """
    from god.index.base import Index

    index_path = plugin_endpoints(name, base_dir)["index"]
    Index(index_path).build()


def initiate_plugin(name: str, base_dir: Union[str, Path, None] = None):
    """Initiate the plugin

    Args:
        name: the name of plugin we wish to know endpoints
        base_dir: the repository path
    """
    build_plugin_directories(name, base_dir)
    build_plugin_index(name, base_dir)


def load_manifest(name: str, base_dir: Union[str, Path, None] = None) -> Dict:
    """Load the plugin manifest

    Args:
        name: the name of plugin we wish to know endpoints
        base_dir: the repository path

    Returns:
        A dict that show the plugin information
    """
    manifest = Path(plugin_endpoints("plugins", base_dir)["tracks"], "manifest", name)

    if not manifest.is_file():
        return {}

    with manifest.open("r") as fi:
        return json.load(fi)["info"]
