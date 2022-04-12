import json
from collections import defaultdict
from pathlib import Path

import click
from rich import print as rprint

from god.configs.add import add
from god.configs.base import Settings, update_config
from god.configs.constants import SYSTEM_CONFIG, USER_CONFIG
from god.configs.init import init
from god.configs.status import status
from god.configs.utils import edit_file
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
@click.option("--system", is_flag=True, default=False, help="System-level config")
@click.option("--user", is_flag=True, default=False, help="User-level config")
@click.option(
    "--local-tree", is_flag=True, default=False, help="Local repo-level config"
)
@click.option("--shared-tree", is_flag=True, default=False, help="Shared repo config")
@click.option("--plugin", type=str, default=None, help="Specify plugins")
def edit(system, user, local_tree, shared_tree, plugin):
    """Edit the config file in YAML format"""

    if system:
        edit_file(SYSTEM_CONFIG)
        return

    if user:
        edit_file(USER_CONFIG)
        return

    shared_path = Path(get_base_dir(), ".god", "workings", "configs", "tracks")
    local_path = Path(get_base_dir(), ".god", "workings", "configs")
    if local_tree:
        if plugin:
            edit_file(str(local_path / "plugins" / plugin))
            return
        else:
            edit_file(str(local_path / "configs"))
            return

    if shared_tree:
        if plugin:
            edit_file(str(shared_path / "plugins" / plugin))
            return
        else:
            edit_file(str(shared_path / "configs"))
            return


@main.command("set")
@click.argument("key")
@click.argument("value")
@click.option("--system", is_flag=True, default=False, help="System-level config")
@click.option("--user", is_flag=True, default=False, help="User-level config")
@click.option(
    "--local-tree", is_flag=True, default=False, help="Local repo-level config"
)
@click.option("--shared-tree", is_flag=True, default=False, help="Shared repo config")
@click.option("--plugin", type=str, default=None, help="Specify plugins")
def set_(key, value, system, user, local_tree, shared_tree, plugin):
    """Set simple key-value config

    Example:
        $ god-config set OTHER_CONFIG.KEY2 "hihihi" --shared-tree --plugin files
    """
    if system:
        update_config(SYSTEM_CONFIG, {key: value})
        return

    if user:
        update_config(USER_CONFIG, {key: value})
        return

    shared_path = Path(get_base_dir(), ".god", "workings", "configs", "tracks")
    local_path = Path(get_base_dir(), ".god", "workings", "configs")
    if local_tree:
        if plugin:
            update_config(str(local_path / "plugins" / plugin), {key: value})
            return
        else:
            update_config(str(local_path / "configs"), {key: value})
            return

    if shared_tree:
        if plugin:
            update_config(str(shared_path / "plugins" / plugin), {key: value})
            return
        else:
            update_config(str(shared_path / "configs"), {key: value})
            return


@main.command("list")
@click.option("--system", is_flag=True, default=False, help="System-level config")
@click.option("--user", is_flag=True, default=False, help="User-level config")
@click.option(
    "--local-tree", is_flag=True, default=False, help="Local repo-level config"
)
@click.option("--shared-tree", is_flag=True, default=False, help="Shared repo config")
@click.option("--plugin", type=str, default=None, help="Specify plugins")
@click.option("--no-plugin", is_flag=True, default=False, help="Ignore plugin config")
@click.option("--pretty", is_flag=True, default=False, help="Whether to pretty print")
def list_(system, user, local_tree, shared_tree, plugin, no_plugin, pretty):
    """List the configurations

    # Returns:
        [Dict]: if `pretty`, the configuration is printed as YAML format (suitable to
            print to console), otherwise, the configuration is printed as JSON format
            (suitable for piping)
    """
    shared_path = Path(get_base_dir(), ".god", "workings", "configs", "tracks")
    local_path = Path(get_base_dir(), ".god", "workings", "configs")

    base_setting = Settings()
    plugins_setting = defaultdict(lambda: Settings(level=2))

    if plugin:
        local_setting, shared_setting = Settings(), Settings()
        if (shared_path / "plugins" / plugin).is_file():
            shared_setting.set_values_from_yaml(shared_path / "plugins" / plugin)
        if (local_path / "plugins" / plugin).is_file():
            local_setting.set_values_from_yaml(local_path / "plugins" / plugin)
        if shared_tree:
            base_setting += shared_setting
        if local_tree:
            base_setting += local_setting
        if not (shared_tree or local_tree):
            base_setting += shared_setting
            base_setting += local_setting
        if pretty:
            print(base_setting)
        else:
            print(json.dumps(base_setting.as_dict()))
        return

    if not (system or user or local_tree or shared_tree):
        system, user, local_tree, shared_tree = True, True, True, True

    if system and Path(SYSTEM_CONFIG).exists():
        base_setting.set_values_from_yaml(SYSTEM_CONFIG)

    if user and Path(USER_CONFIG).exists():
        base_setting.set_values_from_yaml(USER_CONFIG)

    shared_path = Path(get_base_dir(), ".god", "workings", "configs", "tracks")
    if shared_tree:
        base_setting.set_values_from_yaml(shared_path / "configs")
        if not no_plugin:
            for each_path in (shared_path / "plugins").glob("*"):
                plugin_setting = Settings(level=2)
                plugin_setting.set_values_from_yaml(each_path)
                plugins_setting[each_path.name.upper()] += plugin_setting

    local_path = Path(get_base_dir(), ".god", "workings", "configs")
    if local_tree:
        local_setting = Settings()
        local_setting.set_values_from_yaml(local_path / "configs")
        base_setting += local_setting
        if not no_plugin:
            for each_path in (shared_path / "plugins").glob("*"):
                plugin_setting = Settings(level=2)
                plugin_setting.set_values_from_yaml(each_path)
                plugins_setting[each_path.name.upper()] += plugin_setting

    if plugins_setting:
        base_setting.set_values(PLUGINS=plugins_setting)

    # import pdb; pdb.set_trace()
    if pretty:
        print(base_setting)
    else:
        print(json.dumps(base_setting.as_dict()))
