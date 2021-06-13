"""Porcelain commands to be used with CLI"""
import sqlite3
from pathlib import Path

from rich import print

from god.add import add, status
from god.base import settings, read_local_config, update_local_config, read_HEAD
from god.commit import commit
from god.exceptions import InvalidUserParams
from god.init import repo_exists, init
from god.refs import get_ref, update_ref


def init_cmd(path):
    """Initialize the repository

    # Args
        path <str>: directory to initiate `god` repo
    """
    path = Path(path).resolve()
    repo_exists(path)
    init(path)


def config_cmd(op, **kwargs):
    """Local config options

    # Args:
        op <str>: operation with config, can be `list`, `list-local` or `add`
        **kwargs <{str: str}>: the config to add in case `op` is add
    """
    if op == "list":
        return settings

    if op == "list-local":
        return read_local_config(settings.FILE_LOCAL_CONFIG)

    if op == "add":
        update_local_config(settings.FILE_LOCAL_CONFIG, kwargs)


def status_cmd(paths):
    """Viewing repo status"""
    paths = [str(Path(_).resolve()) for _ in paths]
    (
        stage_add,
        stage_update,
        stage_remove,
        add,
        update,
        remove,
        reset_tst,
        unset_mhash,
    ) = status(paths, settings.FILE_INDEX, settings.DIR_BASE)

    refs, snapshot = read_HEAD(settings.FILE_HEAD)
    print(f"On branch {refs}")
    if snapshot:
        print(f"\tUsing snapshot {snapshot}")

    if stage_add or stage_update or stage_remove:
        print("Changes to be commited:")
        for each in stage_add:
            print(f"\t[green]new file:\t{each}[/]")
        for each in stage_update:
            print(f"\t[green]updated:\t{each}[/]")
        for each in stage_remove:
            print(f"\t[green]deleted:\t{each}[/]")
        print()

    if update or remove or unset_mhash:
        print("Changes not staged for commit:")
        for each, _, _ in update:
            print(f"\t[red]updated:\t{each}[/]")
        for each in unset_mhash:
            print(f"\t[red]updated:\t{each}[/]")
        for each in remove:
            print(f"\t[red]deleted:\t{each}[/]")
        print()

    if add:
        print("Untracked files:")
        for each, _, _ in add:
            print(f"\t[red]{each}[/]")
        print()


def add_cmd(paths):
    """Move files in `paths` (recursively) to staging area, ready for commit

    # Args:
        paths <[str]>: list of paths
    """
    if not paths:
        raise InvalidUserParams("Must supply paths to files or directories")

    paths = [str(Path(_).resolve()) for _ in paths]
    add(paths, settings.FILE_INDEX, settings.DEFAULT_DIR_OBJECTS, settings.DIR_BASE)


def commit_cmd(message):
    """Commit the changes in staging area to commit"""
    config = read_local_config(settings.FILE_LOCAL_CONFIG)
    if config.USER is None:
        print("Please config user.name and user.email")
        return

    if config.USER.get("NAME", None) is None:
        print("Please config user.name")
        return

    if config.USER.get("EMAIL", None) is None:
        print("Please config user.email")
        return

    refs, _ = read_HEAD(settings.FILE_HEAD)
    prev_commit = get_ref(refs, settings.DIR_REFS_HEADS)

    current_commit = commit(
        user=config.USER.NAME,
        email=config.USER.EMAIL,
        message=message,
        prev_commit=prev_commit,
        index_path=settings.FILE_INDEX,
        commit_dir=settings.DIR_COMMITS,
        commit_dirs_dir=settings.DIR_COMMITS_DIRECTORY,
    )

    update_ref(refs, current_commit, settings.DIR_REFS_HEADS)
