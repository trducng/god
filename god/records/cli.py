from pathlib import Path

import click



@click.group()
def main():
    pass


@main.command("init")
@click.argument("name")
def init_cmd(name):
    """Initialize record NAME"""
    from god.core.common import get_base_dir
    from god.records.init import init

    init(
        name=name,
        base_dir=str(Path(get_base_dir(), ".god", "workings", "records")),
        force=False,
    )


@main.command("add")
@click.argument("name")
def add_cmd(name):
    """Add record NAME from working to staging area"""
    from god.records.add import add
    add(name)


@main.command("status")
def status_cmd():
    """Update the records"""
    from rich import print as rprint
    from god.records.status import status
    (
        stage_add,
        stage_update,
        stage_remove,
        add,
        update,
        remove,
        _,
        unset_mhash,
    ) = status()

    if stage_add or stage_update or stage_remove:
        rprint("Changes to be commited:")
        for each in stage_add:
            rprint(f"\t[green]new file:\t{each}[/]")
        for each in stage_update:
            rprint(f"\t[green]updated:\t{each}[/]")
        for each in stage_remove:
            rprint(f"\t[green]deleted:\t{each}[/]")
        rprint()

    if update or remove or unset_mhash:
        rprint("Changes not staged for commit:")
        for each, _, _ in update:
            rprint(f"\t[red]updated:\t{each}[/]")
        for each in unset_mhash:
            rprint(f"\t[red]updated:\t{each[0]}[/]")
        for each in remove:
            rprint(f"\t[red]deleted:\t{each}[/]")
        rprint()

    if add:
        rprint("Untracked files:")
        for each, _, _ in add:
            rprint(f"\t[red]{each}[/]")
        rprint()


@main.command("update")
@click.argument("name")
@click.argument("paths", nargs=-1, type=click.Path(exists=True))
@click.option("--set", "set_", multiple=True, type=str, help="col=val")
@click.option("--del", "del_", multiple=True, type=str, help="col1")
def update_cmd(name, paths, set_, del_):
    """Update records information

    Args:
        name: the name of the record
        paths: a list of files
        set_: a list of column and value to update
        del_: a list of column to unset value
    """
    from god.records.configs import RecordsConfig
    from god.records.operations import path_to_record_id
    from god.records.update import update

    config = RecordsConfig(name)

    ids = []
    if paths:
        ids = list(path_to_record_id(paths, config).values())

    if not ids:
        return

    update(
        ids=ids,
        sets=set_,
        dels=del_,
        name=name,
        config=config,
        index_path=settings.FILE_INDEX,
        dir_cache_records=settings.DIR_CACHE_RECORDS,
    )
    from god.hooks.events import post_commit_hook

    post_commit_hook()


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
        return_cols: the list of columns to return
    """
    import csv
    from rich import print as rprint
    from rich.console import Console
    from rich.table import Table

    from god.hooks.events import record_search_hook

    completed_process = record_search_hook(name, queries, columns)
    result = str(completed_process.stdout, encoding="utf-8").strip().splitlines()
    result = list(csv.reader(result))
    if not result:
        return

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


if __name__ == "__main__":
    main()
