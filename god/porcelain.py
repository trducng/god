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
from god.configs.base import settings
from god.core.add import add
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
    from god.configs import get_config

    configs = get_config("configs")
    username = configs.get("user", {}).get("name", None)
    email = configs.get("user", {}).get("email", None)
    if username is None or email is None:
        raise RuntimeError(
            "Missing user's name and/or email. Please set: \n"
            "    $ god configs set user.name=<name>\n"
            "    $ god configs set user.email=<email>\n"
        )

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
            user=username,
            email=email,
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
            user=username,
            email=email,
            include=include,
            exclude=exclude,
        )
        merge_file.unlink()


def fetch_cmd(branch: str, remote: str):
    """Fetch commits from remote to local

    Args:
        branch: the target branch name for us to fetch. If blank, fetch the branch
            has the same branch name as current active local branch
        remote: the specific remote repository that we will fetch from. If blank, use
            the default remote. If default remote has not been set, raise error
    """
    import json
    from pathlib import Path

    from god.fetch import fetch_object_storage
    from god.remote.base import get_default_remote, get_remote

    if not remote:
        remote = get_default_remote(link_path=settings.FILE_LINK)
        if not remote:
            raise RuntimeError(
                "Default remote not found. Please set default remote with:\n"
                "    god remote set [name] --defailt\n"
            )

    if not branch:
        branch, _ = read_HEAD(settings.FILE_HEAD)

    if not branch:
        raise RuntimeError("Please specify branch, or get back from detached mode")

    with open(settings.FILE_LINK, "r") as fi:
        local_path = json.load(fi)["STORAGE"]
    remote_loc = get_remote(link_path=settings.FILE_LINK, name=remote)
    need_apply = fetch_object_storage(
        branch=branch,
        ref_remotes_dir=Path(settings.DIR_REFS_REMOTES, remote),
        remote_path=remote_loc[remote],
        local_path=local_path,
    )
    if need_apply:
        print(f'"Fetched latest commit of "{branch}". Run `god apply` to merge')


def apply_cmd(branch: str, remote: str, method: int):
    """Apply the change from remote branch to current local branch

    Args:
        branch: the name of the branch to apply changes
        remote: the name of the remote
        method: method to apply in case of branch divergence:
            - 0: fast-forward
            - 1: 3-way merge, no fast-forward
            - 2: rebase
    """
    from god.checkout import _checkout_between_commits
    from god.commits.base import get_latest_parent_commit
    from god.remote.base import get_default_remote

    if not remote:
        remote = get_default_remote(link_path=settings.FILE_LINK)
        if not remote:
            raise RuntimeError(
                "Default remote not found. Please set default remote with:\n"
                "    god remote set [name] --defailt\n"
            )

    if not branch:
        branch, _ = read_HEAD(settings.FILE_HEAD)

    if method == 0:
        # can be fast forward if the tip of local inside remote
        commit1 = get_ref(branch, settings.DIR_REFS_HEADS)
        commit2 = get_ref(branch, Path(settings.DIR_REFS_REMOTES, remote))
        if get_latest_parent_commit(commit1, commit2) != commit1:
            raise RuntimeError(
                "Cannot do fast-forward because the tip of local branch "
                "does not show up in remote"
            )
        _checkout_between_commits(commit1, commit2)
        update_ref(branch, commit2, settings.DIR_REFS_HEADS)
    elif method == 1:
        raise NotImplementedError("Hasn't implemented 3-way merge god apply")
    elif method == 2:
        raise NotImplementedError("Hasn't implemented rebase for god apply")

    print("Applied")


def pull_cmd(branch: str, remote: str, method: int):
    """Combine fetch and apply in 1 command"""
    fetch_cmd(branch, remote)
    apply_cmd(branch, remote, method)


def clone_cmd(path, from_: str, location: str):
    """Clone from remote storage to current storage"""
    import json
    import os

    import god.utils.constants as c
    from god.checkout import _checkout_between_commits
    from god.core.refs import get_ref, update_ref
    from god.fetch import fetch_object_storage
    from god.remote.base import set_default_remote, set_remote

    # initialize the repo
    path = Path(path).resolve()
    if path.is_dir():
        repo_exists(path)
    elif path.is_file():
        raise RuntimeError(f'"{path}" is file')
    else:
        path.mkdir(parents=True, exist_ok=True)
    init(path)

    # edit to correct endpoints
    set_remote(
        name="origin",
        location=from_,
        link_path=path / c.FILE_LINK,
        ref_remotes_dir=path / c.DIR_REFS_REMOTES,
    )
    set_default_remote(name="origin", link_path=path / c.FILE_LINK)
    with (path / c.FILE_LINK).open("r") as fi:
        links = json.load(fi)
        links["STORAGE"] = location
    with (path / c.FILE_LINK).open("w") as fo:
        json.dump(links, fo)

    if location == "file://":
        new_location = "file://" + str(path / ".god")
    else:
        new_location = location

    # fetch
    fetch_object_storage(
        branch="main",  # PRIORITY2: don't assume the branch is main
        ref_remotes_dir=str(path / c.DIR_REFS_REMOTES / "origin"),
        remote_path=from_,
        local_path=new_location,
    )

    # apply
    os.chdir(path)
    commit1 = None
    commit2 = get_ref("main", path / c.DIR_REFS_REMOTES / "origin")
    _checkout_between_commits(commit1, commit2)
    update_ref("main", commit2, path / c.DIR_REFS_HEADS)


def push_cmd(branch: str, remote: str):
    """Push the local ref to remote

    Args:
        branch: the branch name to push to remote, if empty, the current branch
        remote: the name of the target remote, if empty, the default one
    """
    from god.push import push_ref
    from god.remote.base import get_default_remote, get_remote

    if not remote:
        remote = get_default_remote(link_path=settings.FILE_LINK)
        if not remote:
            raise RuntimeError(
                "Default remote not found. Please set default remote with:\n"
                "    god remote set [name] --defailt\n"
            )

    if not branch:
        branch, _ = read_HEAD(settings.FILE_HEAD)

    if not branch:
        raise RuntimeError("Please specify branch, or get back from detached mode")

    with open(settings.FILE_LINK, "r") as fi:
        local_path = json.load(fi)["STORAGE"]
    remote_loc = get_remote(link_path=settings.FILE_LINK, name=remote)[remote]

    push_ref(
        ref_name=branch,
        local_ref_path=settings.DIR_REFS_HEADS,
        remote_ref_path=str(Path(settings.DIR_REFS_REMOTES, remote)),
        remote_path=remote_loc,
        local_path=local_path,
    )
