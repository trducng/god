"""Porcelain commands to be used with CLI

# @TODO: porcelain might be moved to cli, because we don't use it anywhere, or make
cli right into porcelain.
"""
import json
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
from god.commits.base import read_commit
from god.core.add import add
from god.core.conf import settings
from god.core.head import read_HEAD
from god.core.refs import get_ref, is_ref, update_ref
from god.core.status import status
from god.init import init, repo_exists
from god.merge import merge, merge_continue
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
    print("Repo initialized")
    print('By default, commited files are store locally with "local" storage. ')
    print("To store them other places, consider `god storages use --help`")


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
    )

    update_ref(refs, current_commit, settings.DIR_REFS_HEADS)


def log_cmd():
    """Print out repository history"""
    refs, _ = read_HEAD(settings.FILE_HEAD)
    commit_id = get_ref(refs, settings.DIR_REFS_HEADS)

    while commit_id:
        commit_obj = read_commit(commit_id)
        rprint(f"[yellow]commit {commit_id}[/]")
        rprint(f"Author: {commit_obj['user']} <{commit_obj['email']}>")
        rprint()
        rprint(f"\t{commit_obj['message']}")
        rprint()
        prev = commit_obj["prev"]
        commit_id = prev[0] if isinstance(prev, (list, int)) else prev


def restore_staged_cmd(paths, plugins=None):
    """Restore files from the staging area to the working area

    # Args:
        paths <[str]>: list of paths
    """
    plugins = [] if plugins is None else [plugins]
    restore_staged(paths, plugins)


def restore_working_cmd(paths, plugins=None):
    """Revert modfiied and deleted files from working area to last commit

    # Args:
        paths <[str]>: list of paths
    """
    plugins = [] if plugins is None else [plugins]
    restore_working(paths, plugins)


def checkout_cmd(branch, new=False):
    """Checkout to a new branch

    # Args
        branch <str>: name of the branch
        new <bool>: whether to create new branch
    """
    if new:
        refs, _ = read_HEAD(settings.FILE_HEAD)
        commit_id = get_ref(refs, settings.DIR_REFS_HEADS)
        checkout_new_branch(
            branch, commit_id, settings.DIR_REFS_HEADS, settings.FILE_HEAD
        )
    else:
        refs, commit1 = read_HEAD(settings.FILE_HEAD)  # start

        branch2 = branch if is_ref(branch, settings.DIR_REFS_HEADS) else None
        # commit2 = is_commit(branch, settings.DIR_COMMITS)

        if branch2 is None:
            raise InvalidUserParams(
                f"Cannot find branch or commit that match '{branch}'"
            )

        checkout(
            settings.DIR_REFS_HEADS,
            settings.FILE_HEAD,
            commit1=commit1,
            commit2=None,
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
        settings.DIR_REFS_HEADS,
        settings.FILE_HEAD,
    )


def merge_cmd(branch, include, exclude, continue_, abort):
    """Perform a merge operation

    # Args:
        branch <str>: name of the branch
    """
    if continue_ and abort:
        raise AttributeError("Cannot --continue and --abort at the same time")

    merge_file = Path(settings.DIR_GOD, "MERGE")
    if continue_:
        if not merge_file.is_file():
            raise RuntimeError("Not in the middle of merge process")
        with merge_file.open("r") as fi:
            merge_progress = json.load(fi)
        refs, commit1 = read_HEAD(settings.FILE_HEAD)
        if merge_progress["ours"]["name"] != refs:
            raise RuntimeError(f"Not in branch {refs}")
        if merge_progress["ours"]["commit"] != commit1:
            raise RuntimeError(
                f"Not in original commit {merge_progress['ours']['commit']}"
            )
        merge_continue(
            merge_progress["ours"]["name"],
            # PRIORITY1: should use commit hash rather than branch name, because
            # the theirs branch might have new commit during the conflict resolution
            merge_progress["theirs"]["name"],
            settings.DIR_REFS_HEADS,
            user="some email",  # @PRIORITY0
            email="some password",
            include=include,
            exclude=exclude,
        )
        merge_file.unlink()
    elif abort:
        if not merge_file.is_file():
            raise RuntimeError("Not in the middle of merge process")
        merge_file.unlink()
        restore_staged(fds=[], plugins=[])
        restore_working(fds=[], plugins=[])
    else:
        refs, commit1 = read_HEAD(settings.FILE_HEAD)
        if merge_file.is_file():
            raise RuntimeError(
                "Seems to be in middle of merge, please 'god merge --abort' first"
            )
        with merge_file.open("w") as fo:
            json.dump(
                {
                    "ours": {"name": refs, "commit": commit1},
                    "theirs": {
                        "name": branch,
                        "commit": get_ref(branch, settings.DIR_REFS_HEADS),
                    },
                },
                fo,
            )
        merge(
            refs,
            branch,
            settings.DIR_REFS_HEADS,
            user="some email",  # @PRIORITY0
            email="some password",
            include=include,
            exclude=exclude,
        )
        merge_file.unlink()
