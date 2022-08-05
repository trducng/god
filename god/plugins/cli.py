import json
from distutils.dir_util import copy_tree
from pathlib import Path

import click

from god.core.common import get_base_dir
from god.plugins.base import (
    build_plugin_directories,
    build_plugin_index,
    installed_plugins,
    load_manifest,
    plugin_endpoints,
)


@click.group()
def main():
    """Plugin components"""
    pass


@main.command("install")
@click.option("-n", "--name", type=str, help="Name of the plugin")
@click.option("-t", "--tar", type=str, help="Path to installation tar file")
@click.option("--store", is_flag=True, default=False)
def install_cmd(name: str, tar: str, store: bool):
    import shutil
    import tempfile

    if not (name or tar):
        raise AttributeError("Must specify either `--name` or --tar`")

    if name and tar:
        raise AttributeError("Must specify either `--name` or --tar`")

    if tar:
        # @TODO: unpack tar file
        path = tempfile.mkdtemp()
        shutil.unpack_archive(tar, path)

        with Path(path, "info.json").open("r") as fi:
            info = json.load(fi)
        plugin_name = info["name"]

        # create working dir and structure in working dir (dev prepares sample folder)
        endpoints = plugin_endpoints(plugin_name)
        build_plugin_directories(plugin_name)
        copy_tree(str(Path(path, "untracks")), endpoints["untracks"])
        copy_tree(str(Path(path, "tracks")), endpoints["tracks"])

        # create blank index
        if info["index"]:
            build_plugin_index(plugin_name)

        # store source, manifest and binary inside plugin's working dir
        plugin_manager_endpoints = plugin_endpoints("plugins")

        src_dir = Path(plugin_manager_endpoints["tracks"], "src")
        src_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(path, src_dir)

        manifest_dir = Path(plugin_manager_endpoints["tracks"], "manifest")
        manifest_dir.mkdir(parents=True, exist_ok=True)
        with (manifest_dir / plugin_name).open("w") as fo:
            json.dump(
                {
                    "name": plugin_name,
                    "hash-type": "sha256",
                    "hash-value": "some-tar-hash",
                    "info": info,
                },
                fo,
            )

        bin_dir = Path(plugin_manager_endpoints["untracks"], "bin")
        bin_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(Path(path, f"god-{plugin_name}"), bin_dir)

        shutil.rmtree(path)

    if name:
        print("Not currently supported with --name, use --tar instead")

    if store:
        print("Not currently supported store")


@main.command("uninstall")
@click.option("-n", "--name", type=str, help="Plugin name")
def uninstall_cmd(name: str):
    """Uninstall plugin

    Uninstalling a plugin involves:
        1. Remove the info.json
        2. Remove the binary
        3. Remove index of `name`
        4. Remove working directory of `name`
        5. Inform about config plugin
    """
    plugin_dir = Path(get_base_dir(), ".god", "workings", "plugins")
    declare_file = plugin_dir / "tracks" / name
    if not declare_file.is_file():
        print(f'Plugin "{name}" does not exist')
    else:
        declare_file.unlink()
        (plugin_dir / "bin" / f"god-{name}").unlink()
        import shutil

        shutil.rmtree(Path(get_base_dir(), ".god", "workings", name))
        Path(get_base_dir(), ".god", "indices", name).unlink()
        print(f"Uninstalled {name}")


@main.command("list")
def list_cmd():
    """@TODO:
    - show if a plugin is (1) installed or not, (2) is stored or not
    - list in such a way that allows piping
    """
    for each in installed_plugins():
        print(each)


@main.command("info")
@click.option("-n", "--name", type=str, help="Plugin name")
@click.option("--endpoints", is_flag=True, help="Return endpoints only")
@click.option("--json", "json_", is_flag=True, help="Return in json format")
def info_cmd(name: str, endpoints: bool, json_: bool):
    """Get plugin info

    Args:
        name: the name of the plugin
        endpoints: whether to retrieve the endpoint
        json: whether to print the result in JSON-friendly format

    Raises:
        PluginNotFound: when plugin not found
    """
    info = plugin_endpoints(name) if endpoints else load_manifest(name)
    if json_:
        print(json.dumps(info))
