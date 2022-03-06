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


def get_index_path_temp(name: str) -> str:
    """We will remove this

    @TODO: use a plugin-method to get index path
    """
    from pathlib import Path

    from god.core.common import get_base_dir

    return str(Path(get_base_dir(), ".god", "indices", name))


@click.group()
def main():
    """Index manager"""
    pass


@main.command("build")
@click.argument("name", type=str)
@click.option("--force", is_flag=True, default=False)
def build(name: str, force: bool):
    """Create the plugin"""
    index_path = get_index_path_temp(name)
    index = Index(index_path)
    index.build(force=force)


@main.command("unbuild")
@click.argument("name", type=str)
def unbuild(name: str):
    """Create the plugin"""
    index_path = get_index_path_temp(name)
    index = Index(index_path)
    index.unbuild()


@main.command("add")
@click.argument("name", type=str)
@click.option("--items", type=click.File("r"), default=sys.stdin)
@click.option("--staged", is_flag=True, default=False)
def add(name: str, items, staged: bool):
    """Create the plugin"""
    index_path = get_index_path_temp(name)
    if items:
        items = json.load(items)
    with Index(index_path) as index:
        index.add(items=items, staged=staged)


@main.command("delete")
@click.argument("name", type=str)
@click.option("--items", type=click.File("r"), default=sys.stdin)
@click.option("--staged", is_flag=True, default=False)
def delete(name: str, items, staged: bool):
    """Create the plugin"""
    index_path = get_index_path_temp(name)
    if items:
        items = json.load(items)
    with Index(index_path) as index:
        index.delete(items=items, staged=staged)


@main.command("revert")
@click.argument("name", type=str)
@click.option("--items", type=click.File("r"), default=sys.stdin)
@click.option("--mhash", is_flag=True, default=False)
@click.option("--remove", is_flag=True, default=False)
def revert(name: str, items, mhash: bool, remove: bool):
    """Create the plugin"""
    index_path = get_index_path_temp(name)
    if items:
        items = json.load(items)
    with Index(index_path) as index:
        index.revert(items=items, mhash=mhash, remove=remove)


@main.command("update")
@click.argument("name", type=str)
@click.option("--items", type=click.File("r"), default=sys.stdin)
def update(name: str, items):
    """Create the plugin"""
    index_path = get_index_path_temp(name)
    if items:
        items = json.load(items)
    with Index(index_path) as index:
        index.update(items=items)


@main.command("get-files")
@click.argument("name", type=str)
@click.option("--names", type=click.File("r"), default=sys.stdin)
@click.option("--get-remove", is_flag=True, default=False)
@click.option("--not-in", is_flag=True, default=False)
def get_files(name: str, names, get_remove: bool, not_in: bool):
    """Create the plugin"""
    index_path = get_index_path_temp(name)
    if names:
        names = json.load(names)
    with Index(index_path) as index:
        result = index.get_files(names=names, get_remove=get_remove, not_in=not_in)
    print(json.dumps(result))


@main.command("get-folder")
@click.argument("name", type=str)
@click.option("--names", type=click.File("r"), default=sys.stdin)
@click.option("--get-remove", is_flag=True, default=False)
def get_folder(name: str, names, get_remove: bool):
    """Create the plugin"""
    index_path = get_index_path_temp(name)
    if names:
        names = json.load(names)
    with Index(index_path) as index:
        result = index.get_folder(names=names, get_remove=get_remove)
    print(json.dumps(result))
