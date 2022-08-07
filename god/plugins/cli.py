import json
from pathlib import Path

import click

from god.core.common import get_base_dir
from god.plugins.base import installed_plugins, load_manifest, plugin_endpoints
from god.plugins.manager import (
    install_plugin_from_gztar_file,
    retrieve_plugin_gztar_file,
)


@click.group()
def main():
    """Plugin components"""
    pass


@main.command("install")
@click.option("-n", "--name", type=str, help="Name of the plugin")
@click.option("-t", "--tar", type=str, help="Path to installation tar file")
def install_cmd(name: str, tar: str):
    if not (name or tar):
        raise AttributeError("Must specify either `--name` or --tar`")

    if name and tar:
        raise AttributeError("Must specify either `--name` or --tar`")

    store_src = True
    if name:
        tar = retrieve_plugin_gztar_file(name=name)
        store_src = False

    install_plugin_from_gztar_file(tar=tar, store_src=store_src)


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
