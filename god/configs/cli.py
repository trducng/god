import json
from pathlib import Path

import click
from rich import print as rprint

from god.configs import (
    ConfigLevel,
    edit_config_file,
    get_config,
    get_config_at_specific_level,
    update_config,
)
from god.configs.add import add
from god.configs.init import init
from god.configs.status import status
from god.core.common import get_base_dir


@click.group()
def main():
    """Configuration components"""
    pass


@main.command("init")
@click.option("--force", is_flag=True, default=False)
def init_cmd(force):
    working_dir = Path(get_base_dir(), ".god", "workings", "configs")
    init(str(working_dir), force)


@main.command("add")
@click.argument("name")
def add_(name):
    add(name)


@main.command("status")
def status_():
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


@main.command("edit")
@click.option("--plugin", type=str, default="configs", help="Specify plugins")
@click.option(
    "--level",
    type=click.Choice(ConfigLevel.list()),
    default="local",
    help="Specify the level of config. "
    "Possible values are: local (default), system, user, shared.",
)
def edit(plugin: str, level: str):
    """Edit the config file in YAML format"""
    edit_config_file(plugin, level)


@main.command("set")
@click.argument("key")
@click.argument("value")
@click.option("--plugin", type=str, default="configs", help="Specify plugins")
@click.option(
    "--level",
    type=click.Choice(ConfigLevel.list()),
    default="local",
    help="Specify the level of config. "
    "Possible values are: local (default), system, user, shared.",
)
def set_(key: str, value: str, plugin: str, level: str):
    """Set simple key-value config

    Example:
        $ god-configs set user.name "trducng"
    """
    update_config(plugin=plugin, level=level, config_dict={key: value})


@main.command("list")
@click.option("--plugin", type=str, default="configs", help="Specify plugins")
@click.option(
    "--level",
    type=click.Choice(ConfigLevel.list()),
    default=None,
    help="Specify the level of config. If blank (default), will aggregated config "
    "value across levels. Otherwise, possible values are: system, user, shared, local.",
)
@click.option("--pretty", is_flag=True, default=False, help="Whether to pretty print")
def list_(plugin, level, pretty):
    """List the configurations

    # Returns:
        [Dict]: if `pretty`, the configuration is printed as YAML format (suitable to
            print to console), otherwise, the configuration is printed as JSON format
            (suitable for piping)
    """
    setting = (
        get_config(plugin)
        if level is None
        else get_config_at_specific_level(plugin=plugin, level=level)
    )

    if pretty:
        print(setting)
    else:
        print(json.dumps(setting.as_dict()))
