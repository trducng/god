"""Extensible plugin management

The plugin manager organizes tracks and untracks directory as follow:
    .
    |__tracks/
    |   |__ manifest/
    |   |   |__ plugin1
    |   |   |__ plugin2
    |   |__ src/
    |       |__ plugin1
    |       |__ plugin2
    |__untracks/
       |__ bin/
           |__ god-plugin1
           |__ god-plugin2
"""
import hashlib
import json
import shutil
import tempfile
from distutils.dir_util import copy_tree
from pathlib import Path
from typing import Union

from god.plugins.base import (
    build_plugin_directories,
    build_plugin_index,
    load_manifest,
    plugin_endpoints,
)
from god.utils.exceptions import NotYetSupported
from god.utils.process import communicate

BUILTIN_PLUGINS = {"records", "snapshots"}


def install_plugin_from_gztar_file(
    tar: Union[str, Path],
    store_src: bool = False,
    base_dir: Union[str, Path, None] = None,
):
    """Install the plugin from path"""
    with open(tar, "rb") as fi:
        tar_hash = hashlib.sha256(fi.read()).hexdigest()
    path = tempfile.mkdtemp()
    shutil.unpack_archive(tar, path, format="gztar")

    with Path(path, "info.json").open("r") as fi:
        info = json.load(fi)
        plugin_name = info["name"]

    # create working dir and structure in working dir (dev prepares sample folder)
    endpoints = plugin_endpoints(plugin_name, base_dir=base_dir)
    build_plugin_directories(plugin_name)
    copy_tree(str(Path(path, "untracks")), endpoints["untracks"])
    copy_tree(str(Path(path, "tracks")), endpoints["tracks"])

    # create blank index
    if info["index"]:
        build_plugin_index(plugin_name)

    # store source, manifest and binary inside plugin's working dir
    plugin_manager_endpoints = plugin_endpoints("plugins", base_dir=base_dir)

    if store_src:
        src_dir = Path(plugin_manager_endpoints["tracks"], "src")
        src_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(path, src_dir / plugin_name)

    manifest_dir = Path(plugin_manager_endpoints["tracks"], "manifest")
    manifest_dir.mkdir(parents=True, exist_ok=True)
    with (manifest_dir / plugin_name).open("w") as fo:
        json.dump(
            {
                "hash-type": "sha256",
                "hash-value": tar_hash,
                "info": info,
            },
            fo,
        )

    bin_dir = Path(plugin_manager_endpoints["untracks"], "bin")
    bin_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(Path(path, f"god-{plugin_name}"), bin_dir)

    # custom post-install
    if "postinstall" in info.get("commands", {}):
        communicate(info["commands"]["postinstall"])

    if info.get("config"):
        from god.configs import update_config
        from god.configs.utils import ConfigLevel

        update_config(
            plugin=plugin_name,
            level=ConfigLevel.SHARED,
            config_dict=info["config"],
            base_dir=base_dir,
        )

    shutil.rmtree(path)


def retrieve_plugin_gztar_file(
    name: str, base_dir: Union[str, Path, None] = None
) -> str:
    """Retrieve the plugin tar file for a known plugin in a repository

    For a known plugin in a repository, retrieve the plugin tar file

    Returns:
        Directory to the tar file
    """
    if name in BUILTIN_PLUGINS:
        return str(Path(__file__).parent / "builtins" / f"{name}")

    plugin_manager_endpoints = plugin_endpoints("plugins", base_dir=base_dir)
    tar = Path(plugin_manager_endpoints["tracks"], "src", f"{name}.tar.gz")
    if tar.is_file():
        return str(tar)

    raise NotYetSupported("get plugin from plugin marketplace")


def awake_passive_plugin(name: str, base_dir: Union[str, Path, None] = None):
    """Initialize inactive plugin

    Passive pluins situation usually happens when a repository is cloned, or
    when a repo is pull where remote exists newer plugin. The plugins will have
    downloaded files. However, the executable binary file will not be set up,
    and plugin-specific initialization scripts aren't run. Hence, after `git
    clone`, we need to initialize available plugins.

    Args:
        name: the name of plugin
        base_dir: the repository directory
    """
    tar = retrieve_plugin_gztar_file(name, base_dir)
    path = tempfile.mkdtemp()
    shutil.unpack_archive(tar, path, format="gztar")

    endpoints = plugin_endpoints(name, base_dir=base_dir)
    plugin_manager_endpoints = plugin_endpoints("plugins", base_dir=base_dir)

    # initiate the untrack directory
    copy_tree(str(Path(path, "untracks")), endpoints["untracks"])

    # setup executable binary
    bin_dir = Path(plugin_manager_endpoints["untracks"], "bin")
    bin_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(Path(path, f"god-{name}"), bin_dir)

    # run the post-installation script
    plugin_manifest = load_manifest(name, base_dir=base_dir)
    if "postinstall" in plugin_manifest.get("commands", {}):
        communicate(plugin_manifest["commands"]["postinstall"])
