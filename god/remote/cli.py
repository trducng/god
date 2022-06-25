import json

import click

from god.configs.base import settings
from god.remote.base import (
    get_default_remote,
    get_remote,
    set_default_remote,
    set_remote,
    unset_default_remote,
    unset_remote,
)


@click.group()
def main():
    """Remote component"""
    pass


@main.command("get")
@click.option(
    "-n",
    "--name",
    type=str,
    help="Name of the remote to get. If blank, get all",
    default="",
)
@click.option(
    "--default",
    is_flag=True,
    default=False,
    help="Get the name and location of default remote",
)
def get_cmd(name: str, default: bool):
    """Get remote information"""
    settings.set_global_settings()
    if default:
        remote_name = get_default_remote(link_path=settings.FILE_LINK)
        print(json.dumps(get_remote(link_path=settings.FILE_LINK, name=remote_name)))
        return

    print(json.dumps(get_remote(link_path=settings.FILE_LINK, name=name)))


@main.command("set")
@click.argument("name", type=str)
@click.argument("location", type=str, default="", required=False)
@click.option(
    "--default",
    is_flag=True,
    default=False,
    help="Set the specified remote as default",
)
def set_cmd(name: str, location: str, default: bool):
    """Set remote information"""
    settings.set_global_settings()
    if location:
        set_remote(
            name=name,
            location=location,
            link_path=settings.FILE_LINK,
            ref_remotes_dir=settings.DIR_REFS_REMOTES,
        )

    if default or len(get_remote(link_path=settings.FILE_LINK)) == 1:
        set_default_remote(name=name, link_path=settings.FILE_LINK)


@main.command("unset")
@click.argument("name", type=str, default="")
@click.option(
    "--default",
    is_flag=True,
    default=False,
    help="Remove the remote as default remote only",
)
def unset_cmd(name: str, default: bool):
    """Unset remote from local"""
    settings.set_global_settings()
    if name:
        unset_remote(
            name=name,
            link_path=settings.FILE_LINK,
            ref_remotes_dir=settings.DIR_REFS_REMOTES,
        )

    if default or get_default_remote(link_path=settings.FILE_LINK) == name:
        unset_default_remote(link_path=settings.FILE_LINK)
