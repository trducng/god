from pathlib import Path

import click

from god.core.conf import settings
from god.plugins.cli import main as plugin_cli
from god.configs.cli import main as config_cli
from god.porcelain import (
    add_cmd,
    checkout_cmd,
    commit_cmd,
    config_cmd,
    init_cmd,
    log_cmd,
    merge_cmd,
    reset_cmd,
    restore_staged_cmd,
    restore_working_cmd,
    status_cmd,
)


class DynamicGroup(click.Group):
    def get_command(self, ctx, cmd_name):
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv

        return click.Group.get_command(self, ctx, "execute-plugin")

    def resolve_command(self, ctx, args):
        _, cmd, new_args = super().resolve_command(ctx, args)
        if cmd.name == "execute-plugin":
            return cmd.name, cmd, args

        return cmd.name, cmd, new_args


@click.command(cls=DynamicGroup)
def main():
    """god is the git of data"""
    pass


# For testing
@main.command("execute-plugin", context_settings=dict(ignore_unknown_options=True))
@click.argument("extra_args", nargs=-1, type=click.UNPROCESSED)
def execute_plugin(extra_args):
    import os
    import subprocess
    from pathlib import Path

    from god.core.common import get_base_dir

    # subprocess.run([])
    plugin_bin = Path(get_base_dir(), ".god", "workings", "plugins", "bin")
    executable = f"god-{extra_args[0]}"
    if not (plugin_bin / executable).is_file():
        print(f'Please make sure plugin "{executable}" is installed')
    else:
        cmd = list(extra_args)
        cmd[0] = f"god-{extra_args[0]}"
        my_env = os.environ.copy()
        my_env["PATH"] = f"{str(plugin_bin)}:" + my_env["PATH"]
        subprocess.run(cmd, env=my_env)


# For testing
@main.command("test")
def main_test():
    from god.hooks.events import post_commit

    post_commit()


# 1. Main group
@main.command("init")
@click.argument("path", default=".")
def init(path):
    """Initialize the repo

    PATH is the repo directory. If not specified, use current working directory.
    """
    init_cmd(path)


@main.command("status")
@click.argument("paths", nargs=-1, type=click.Path(exists=True))
def status(paths):
    """Show the working tree status

    Display the changes and what would be commited, including files and records.
    """
    settings.set_global_settings()
    if not paths:
        paths = (str(Path.cwd()),)
    status_cmd(paths)


@main.command("add")
@click.argument("paths", nargs=-1, type=click.Path(exists=True))
def add(paths):
    """Add files from working directory to staging. Add all records to staging.

    This is an umbrella command for `god files add` and `god records add`. Add
    specific files from working area to staging area. Add all records from working
    area to staging area.
    """
    settings.set_global_settings()
    add_cmd(paths)


@main.command("commit")
@click.option("-m", "--message", required=True, type=str, help="Commit message")
def commit(message):
    """Commit changes from staging area"""
    settings.set_global_settings()
    commit_cmd(message)


@main.command("log")
def log():
    """Show commit history"""
    settings.set_global_settings()
    log_cmd()


@main.command("restore")
@click.argument("paths", nargs=-1, type=click.Path(exists=True), required=True)
@click.option(
    "-s",
    "--staged",
    is_flag=True,
    help="Revert from staging to working area. Else, revert from working area to latest commit.",
    default=False,
)
def restore(paths, staged):
    """Restore modified files"""
    settings.set_global_settings()
    if staged:
        restore_staged_cmd(paths)
    else:
        restore_working_cmd(paths)


@main.command("reset")
@click.argument("head_past")
@click.option(
    "--hard",
    is_flag=True,
    help="Complete reset to previous commit. Otherwise, keep the changes in working area",
    default=False,
)
def reset(head_past, hard):
    """Reset the repository to previous commit"""
    settings.set_global_settings()
    reset_cmd(head_past, hard)


@main.command("checkout")
@click.argument("branch")
@click.option("-n", "--new", is_flag=True, help="Create new branch", default=False)
def checkout(branch, new):
    """Checkout to a branch"""
    settings.set_global_settings()
    checkout_cmd(branch, new)


@main.command("merge")
@click.argument("branch")
def merge(branch):
    """Merge current branch to branch BRANCH"""
    settings.set_global_settings()
    merge_cmd(branch)


main.add_command(plugin_cli, "plugins")
main.add_command(config_cli, "configs")


def entrypoint():
    """Exception handling"""
    try:
        main()
    except Exception as e:
        click.echo(e)
