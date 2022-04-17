import json
import sys
from pathlib import Path

import click

from god.core.common import get_base_dir
from god.index.base import Index
from god.plugins.utils import installed_plugins, plugin_endpoints


@click.group()
def main():
    """Plugin components"""
    pass


@main.command("install")
@click.option("-n", "--name", type=str, help="Name of the plugin")
@click.option("-p", "--path", type=str, help="Path to installation tar file")
@click.option("--store", is_flag=True, default=False)
def install_cmd(name: str, path: str, store: bool):
    import shutil

    if not (name or path):
        raise AttributeError("Must specify either `--name` or --path`")

    if name and path:
        raise AttributeError("Must specify either `--name` or --path`")

    if path:
        # @TODO: unpack tar file

        with Path(path, "info.json").open("r") as fi:
            info = json.load(fi)
        plugin_name = info["name"]

        # create working dir and structure in working dir (dev prepares sample folder)
        working_dir = Path(get_base_dir(), ".god", "workings", plugin_name)
        working_dir.mkdir(exist_ok=True, parents=True)

        untrack_dir = working_dir / "untracks"
        ori_untracks = Path(path, "untracks")
        if ori_untracks.is_dir():
            shutil.copytree(ori_untracks, untrack_dir)
        else:
            untrack_dir.mkdir(exist_ok=True, parents=True)

        track_dir = working_dir / "tracks"
        ori_tracks = Path(path, "tracks")
        if ori_tracks.is_dir():
            shutil.copytree(ori_tracks, track_dir)
        else:
            track_dir.mkdir(exist_ok=True, parents=True)

        # create blank index
        if info["index"]:
            index_path = Path(get_base_dir(), ".god", "indices", plugin_name)
            Index(index_path).build(force=True)

        # store binary and manifest inside plugin's working dir
        plugin_dir = Path(get_base_dir(), ".god", "workings", "plugins")
        shutil.copy(Path(path, f"god-{plugin_name}"), plugin_dir / "bin")
        with (plugin_dir / "tracks" / plugin_name).open("w") as fo:
            json.dump(
                {
                    "name": plugin_name,
                    "hash-type": "sha256",
                    "hash-value": "some-tar-hash",
                    "info": info,
                },
                fo,
            )

    if name:
        print("Not currently supported with --name, use --path instead")

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
def info_cmd(name, endpoints, json_):
    """Get plugin info"""
    info = {}

    if endpoints:
        info.update(plugin_endpoints(name))
        if json_:
            print(json.dumps(info))
        sys.exit(0)

    plugin_info = Path(plugin_endpoints("plugins")["tracks"], name)
    if not plugin_info.is_file():
        print(f'Plugin "{name}" does not exist')
        sys.exit(1)

    with plugin_info.open("r") as fi:
        info.update(json.load(fi))

    if json_:
        print(json.dumps(info))
