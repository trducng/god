"""Porcelain commands to be used with CLI"""
import sqlite3
from pathlib import Path

from rich import print

from god.branches import (
    add,
    status,
    restore_staged,
    restore_working,
    checkout,
    checkout_new_branch,
    reset,
    merge
)
from god.base import settings, read_local_config, update_local_config, read_HEAD
from god.commit import commit, read_commit, is_commit
from god.exceptions import InvalidUserParams
from god.init import repo_exists, init
from god.refs import get_ref, update_ref, is_ref


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

    refs, snapshot, commits = read_HEAD(settings.FILE_HEAD)

    if refs:
        print(f"On branch {refs}")
    if commits:
        print(f"On detached commit {commits}")
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

    refs, _, _ = read_HEAD(settings.FILE_HEAD)
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


def log_cmd():
    """Print out repository history"""
    refs, _, _ = read_HEAD(settings.FILE_HEAD)
    commit_id = get_ref(refs, settings.DIR_REFS_HEADS)

    while commit_id:
        commit_obj = read_commit(commit_id, settings.DIR_COMMITS)
        print(f"[yellow]commit {commit_id}[/]")
        print(f"Author: {commit_obj['user']} <{commit_obj['email']}>")
        print()
        print(f"\t{commit_obj['message']}")
        print()
        prev = commit_obj["prev"]
        commit_id = prev[0] if isinstance(prev, (list, int)) else prev


def restore_staged_cmd(paths):
    """Restore files from the staging area to the working area

    # Args:
        paths <[str]>: list of paths
    """
    if not paths:
        raise InvalidUserParams("Must supply paths to files or directories")

    paths = [str(Path(_).resolve()) for _ in paths]
    restore_staged(
        paths, settings.FILE_INDEX, settings.DEFAULT_DIR_OBJECTS, settings.DIR_BASE
    )


def restore_working_cmd(paths):
    """Revert modfiied and deleted files from working area to last commit

    # Args:
        paths <[str]>: list of paths
    """
    if not paths:
        raise InvalidUserParams("Must supply paths to files or directories")

    paths = [str(Path(_).resolve()) for _ in paths]
    restore_working(
        paths, settings.FILE_INDEX, settings.DEFAULT_DIR_OBJECTS, settings.DIR_BASE
    )


def checkout_cmd(branch, new=False):
    """Checkout to a new branch

    # Args
        branch <str>: name of the branch
        new <bool>: whether to create new branch
    """
    if new:
        refs, _, _ = read_HEAD(settings.FILE_HEAD)
        commit_id = get_ref(refs, settings.DIR_REFS_HEADS)
        checkout_new_branch(
            branch, commit_id, settings.DIR_REFS_HEADS, settings.FILE_HEAD
        )
    else:
        refs, _, commit1 = read_HEAD(settings.FILE_HEAD)  # start

        branch2 = branch if is_ref(branch, settings.DIR_REFS_HEADS) else None
        commit2 = is_commit(branch, settings.DIR_COMMITS)

        if commit2 is None and branch2 is None:
            raise InvalidUserParams(
                f"Cannot find branch or commit that match '{branch}'"
            )

        checkout(
            settings.DIR_COMMITS,
            settings.DIR_COMMITS_DIRECTORY,
            settings.FILE_INDEX,
            settings.DEFAULT_DIR_OBJECTS,
            settings.DIR_REFS_HEADS,
            settings.DIR_BASE,
            settings.FILE_HEAD,
            commit1=commit1,
            commit2=commit2,
            branch1=refs,
            branch2=branch2,
        )


def reset_cmd(head_past, hard=False):
    """Reset branch to previous commit

    # Args:
        head_past <str>: the head past, of format HEAD^x, where x is an integer
        hard <bool>: if true, complete convert to commit_id
    """
    head_past = int(head_past.split("^")[-1])
    reset(
        head_past,
        hard,
        settings.DIR_COMMITS,
        settings.DIR_COMMITS_DIRECTORY,
        settings.FILE_INDEX,
        settings.DEFAULT_DIR_OBJECTS,
        settings.DIR_REFS_HEADS,
        settings.DIR_BASE,
        settings.FILE_HEAD,
    )


def merge_cmd(branch):
    """Perform a merge operation

    # Args:
        branch <str>: name of the branch
    """
    refs, _, commit1 = read_HEAD(settings.FILE_HEAD)
    config = read_local_config(settings.FILE_LOCAL_CONFIG)
    merge(
        refs,
        branch,
        settings.DIR_REFS_HEADS,
        settings.DIR_COMMITS,
        settings.DIR_COMMITS_DIRECTORY,
        settings.FILE_INDEX,
        settings.DEFAULT_DIR_OBJECTS,
        settings.DIR_BASE,
        user=config.USER.NAME,
        email=config.USER.EMAIL,
    )

