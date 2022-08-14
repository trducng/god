"""Index-related functionality

For the plugins to use the index, it can apply the plugin name, as follow:
    god index add --name <plugin-name>

Avoid receiving the config... directly, because there should be 1 definitive way
to read config files.
"""
import json
import sys

import click

from god.index.base import Index
from god.index.trackchanges import (
    track_files,
    track_staging_changes,
    track_working_changes,
)
from god.plugins.base import plugin_endpoints
from god.utils.process import str_stdin_option


@click.group()
def main():
    """Index manager"""
    pass


@main.command("build")
@click.argument("name", type=str)
@click.option("--force", is_flag=True, default=False)
def build(name: str, force: bool):
    """Construct the index"""
    index_path = plugin_endpoints(name)["index"]
    index = Index(index_path)
    index.build(force=force)


@main.command("unbuild")
@click.argument("name", type=str)
def unbuild(name: str):
    """Delete the index"""
    index_path = plugin_endpoints(name)["index"]
    index = Index(index_path)
    index.unbuild()


@main.command("add")
@click.argument("name", type=str)
@click.option("--items", "items_in", type=str_stdin_option, default=sys.stdin)
@click.option("--staged", is_flag=True, default=False)
def add(name: str, items_in: str, staged: bool):
    """Add entries to the index"""
    index_path = plugin_endpoints(name)["index"]
    items = json.loads(items_in)
    with Index(index_path) as index:
        index.add(items=items, staged=staged)


@main.command("delete")
@click.argument("name", type=str)
@click.option("--items", "items_in", type=str_stdin_option, default=sys.stdin)
@click.option("--staged", is_flag=True, default=False)
def delete(name: str, items_in: str, staged: bool):
    """Delete entries from the index"""
    index_path = plugin_endpoints(name)["index"]
    items = json.loads(items_in)
    with Index(index_path) as index:
        index.delete(items=items, staged=staged)


@main.command("revert")
@click.argument("name", type=str)
@click.option("--items", "items_in", type=str_stdin_option, default=sys.stdin)
@click.option("--mhash", is_flag=True, default=False)
@click.option("--remove", is_flag=True, default=False)
def revert(name: str, items_in: str, mhash: bool, remove: bool):
    """Revert entries to the original version"""
    index_path = plugin_endpoints(name)["index"]
    items = json.loads(items_in)
    with Index(index_path) as index:
        index.revert(items=items, mhash=mhash, remove=remove)


@main.command("update")
@click.argument("name", type=str)
@click.option("--items", "items_in", type=str_stdin_option, default=sys.stdin)
def update(name: str, items_in: str):
    """Update information of the newest entries"""
    index_path = plugin_endpoints(name)["index"]
    items = json.loads(items_in)
    with Index(index_path) as index:
        index.update(items=items)


@main.command("get-files")
@click.argument("name", type=str)
@click.option("--names", "names_in", type=str_stdin_option, default=sys.stdin)
@click.option("--get-remove", is_flag=True, default=False)
@click.option("--not-in", is_flag=True, default=False)
def get_files(name: str, names_in: str, get_remove: bool, not_in: bool):
    """Get entries as files"""
    index_path = plugin_endpoints(name)["index"]
    names = json.loads(names_in)
    with Index(index_path) as index:
        result = index.get_files(names=names, get_remove=get_remove, not_in=not_in)
    print(json.dumps(result))


@main.command("get-folder")
@click.argument("name", type=str)
@click.option("--names", "names_in", type=str_stdin_option, default=sys.stdin)
@click.option("--get-remove", is_flag=True, default=False)
@click.option("--conflict", is_flag=True, default=False)
def get_folder(name: str, names_in: str, get_remove: bool, conflict: bool):
    """Get entries as folder"""
    index_path = plugin_endpoints(name)["index"]
    names = json.loads(names_in)
    with Index(index_path) as index:
        result = index.get_folder(names=names, get_remove=get_remove, conflict=conflict)
    print(json.dumps(result))


@main.command("track")
@click.argument("name", type=str)
@click.option("--fds", "fds_in", type=str_stdin_option, default=sys.stdin)
@click.option("--staging", is_flag=True, default=False)
@click.option("--working", is_flag=True, default=False)
def track(name: str, fds_in: str, staging: bool, working: bool):
    """Get entries as folder"""
    endpoints = plugin_endpoints(name)
    index_path = endpoints["index"]
    base_dir = endpoints["base_dir"]
    fds = json.loads(fds_in)

    if staging and not working:
        result = track_staging_changes(fds, index_path, base_dir)
    elif working and not staging:
        result = track_working_changes(fds, index_path, base_dir)
    else:
        result = track_files(fds, index_path, base_dir)

    print(json.dumps(result))
