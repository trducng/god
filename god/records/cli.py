import json
import sys
from pathlib import Path
from typing import List

import click
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from god.records.configs import RecordsConfig
from god.records.hooks import poststatus, preadd, prestatus
from god.records.init import init
from god.records.operations import path_to_record_id
from god.records.sqlite import SQLiteTable
from god.records.update import update
from god.records.utils import communicate, error, get_endpoints, resolve_paths


@click.group()
def main():
    """Organize files into SQL-like database for easier search"""
    pass


@main.command("init")
@click.argument("name", type=str)
def init_cmd(name):
    """Initialize record `name`"""

    init(
        name=name,
        base_dir=get_endpoints()["tracks"],
        force=False,
    )


@main.command("update")
@click.argument("name")
@click.argument("paths", nargs=-1, type=click.Path(exists=True))
@click.option("--set", "set_", multiple=True, type=str, help="col=val")
@click.option("--del", "del_", multiple=True, type=str, help="col1")
def update_cmd(name, paths, set_, del_):
    """Update records information

    Example:
        $ god records update <index> <file-path> \
                --set "col1=val1" --set "col2+=val2" --set "col3-=val3"
        $ god records update <index> <file-path> \
                --del col1 --del col2

    Args:
        name: the name of the record
        paths: a list of files
        set_: a list of column and value to update
        del_: a list of column to unset value
    """
    config = RecordsConfig(name)
    endpoints = get_endpoints()

    ids = []
    if paths:
        paths = resolve_paths(
            [str(Path(_).resolve()) for _ in paths], endpoints["base_dir"]
        )
        ids = list(path_to_record_id(paths, config).values())

    if not ids:
        error("Requires path", statuscode=1)

    update(
        ids=ids,
        sets=set_,
        dels=del_,
        config=config,
        dir_records=str(Path(endpoints["tracks"], name)),
    )


@main.command("search")
@click.argument("name")
@click.option(
    "-q", "--query", "queries", multiple=True, type=str, help="Search query (col=val)"
)
@click.option(
    "-c", "--col", "columns", multiple=True, type=str, help="Column to return"
)
@click.option("--pager", is_flag=True, default=False, help="Use pager")
def search_cmd(name: str, queries: list, columns: list, pager: bool):
    """Search the records

    This command prints result to the console.

    Args:
        name: the name of the record
        queries: the conditions to return result
        columns: the list of columns to return
        pager: whether to use pager (e.g. `less`)
    """
    dir_db = get_endpoints()["untracks"]
    if not Path(dir_db, name).is_file():
        error(
            message=(
                f'Database for record "{name}" doesn\'t exist. '
                f"Please run `god records refresh {name}`."
            ),
            statuscode=1,
        )

    with SQLiteTable(dir_db=dir_db, name=name) as table:
        result = table.search(queries, columns)

    table = Table(show_header=True)
    for each_column in result[0]:
        table.add_column(each_column)
    for each_row in result[1:]:
        table.add_row(*each_row)

    if pager:
        console = Console()
        with console.pager():
            console.print(table)
    else:
        rprint(table)


@main.group("hook")
def hook():
    """Internal hooks"""
    pass


@hook.command("prestatus")
def hook_prestatus_cmd():
    """Convert record name to internal path"""
    input_ = sys.stdin.read().strip()
    fds = json.loads(input_)
    print(json.dumps(prestatus(fds)))


@hook.command("poststatus")
def hook_poststatus_cmd():
    """Convert internal path to record name"""
    input_ = sys.stdin.read().strip()
    file_status = json.loads(input_)
    print(json.dumps(poststatus(file_status)))


@hook.command("preadd")
def hook_preadd_cmd():
    """Organize records internal and leaf folders to contain only relevant files"""
    input_ = sys.stdin.read().strip()
    roots = json.loads(input_)
    for root in roots:
        base_dir = str(Path(get_endpoints()["tracks"], root))
        preadd(base_dir)

    print(input_)


@main.command("build")
@click.argument("name", type=str)
def db_build_cmd(name: str):
    """Build empty database"""
    dir_db = str(Path(get_endpoints()["untracks"]))
    config = RecordsConfig(name)

    with SQLiteTable(dir_db, name) as table:
        table.create_record_db(config=config)


@main.command("refresh")
@click.argument("name", type=str)
def db_refresh_cmd(name: str):
    """Populate database"""
    endpoints = get_endpoints()
    dir_db = str(Path(endpoints["untracks"]))
    dir_records = str(Path(endpoints["tracks"], name))

    with Path(dir_records, "root").open("r") as fi:
        root = fi.read().strip()

    # get all files and hashes (as recorded by the index)
    files_info: List = communicate(
        command=["god-index", "get-folder", "files"], stdin=["."]
    )  # type: ignore
    files_hashes = {_[0]: _[1] or _[2] for _ in files_info}

    config = RecordsConfig(name)
    with SQLiteTable(dir_db, name) as table:
        table.construct_record(
            config=config, files_hashes=files_hashes, root=root, dir_record=dir_records
        )


if __name__ == "__main__":
    main()
