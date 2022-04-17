"""Porcelain commands to be used with CLI

# @TODO: porcelain might be moved to cli, because we don't use it anywhere, or make
cli right into porcelain.
"""
from pathlib import Path

from rich import print as rprint

from god.checkout import (
    checkout,
    checkout_new_branch,
    reset,
    restore_staged,
    restore_working,
)
from god.commit import commit
from god.commits.base import is_commit, read_commit
from god.core.add import add
from god.core.conf import read_local_config, settings
from god.core.head import read_HEAD
from god.core.refs import get_ref, is_ref, update_ref
from god.core.status import status
from god.init import init, repo_exists
from god.merge import merge
from god.utils.exceptions import InvalidUserParams
from god.utils.process import communicate


def init_cmd(path):
    """Initialize the repository

    # Args
        path <str>: directory to initiate `god` repo
    """
    path = Path(path).resolve()
    repo_exists(path)
    init(path)


def status_cmd(paths, plugins):
    """Viewing repo status"""
    refs, commits = read_HEAD(settings.FILE_HEAD)

    if refs:
        rprint(f"On branch {refs}")
    if commits:
        rprint(f"On detached commit {commits}")

    for plugin_name, (
        stage_add,
        stage_update,
        stage_remove,
        add_,
        update,
        remove,
        _,
        unset_mhash,
    ) in status(paths, plugins).items():

        rprint(f"Plugin {plugin_name}")
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

        if add_:
            rprint("Untracked files:")
            for each, _, _ in add_:
                rprint(f"\t[red]{each}[/]")
            rprint()


def add_cmd(paths, plugin):
    """Move files in `paths` (recursively) to staging area, ready for commit

    # Args:
        paths <[str]>: list of paths
    """
    if not paths:
        raise InvalidUserParams("Must supply paths to files or directories")

    add(
        fds=paths,
        plugin=plugin,
    )


def commit_cmd(message):
    """Commit the changes in staging area to commit"""
    config: dict = communicate(
        ["god", "configs", "list", "--user", "--no-plugin"]
    )  # type: ignore

    if config.get("USER", None) is None:
        print("Please config USER.NAME and USER.EMAIL")
        return

    if config["USER"].get("NAME", None) is None:
        print("Please config user.name")
        return

    if config["USER"].get("EMAIL", None) is None:
        print("Please config user.email")
        return

    refs, _ = read_HEAD(settings.FILE_HEAD)
    prev_commit = get_ref(refs, settings.DIR_REFS_HEADS)

    current_commit = commit(
        user=config["USER"]["NAME"],
        email=config["USER"]["EMAIL"],
        message=message,
        prev_commit=prev_commit,
        commit_dir=settings.DIR_COMMITS,
        commit_dirs_dir=settings.DIR_DIRS,
    )

    update_ref(refs, current_commit, settings.DIR_REFS_HEADS)


def log_cmd():
    """Print out repository history"""
    refs, _, _ = read_HEAD(settings.FILE_HEAD)
    commit_id = get_ref(refs, settings.DIR_REFS_HEADS)

    while commit_id:
        commit_obj = read_commit(commit_id, settings.DIR_COMMITS)
        rprint(f"[yellow]commit {commit_id}[/]")
        rprint(f"Author: {commit_obj['user']} <{commit_obj['email']}>")
        rprint()
        rprint(f"\t{commit_obj['message']}")
        rprint()
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
