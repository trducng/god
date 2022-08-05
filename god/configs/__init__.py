from pathlib import Path
from typing import Dict, Union

from god.configs.base import Settings
from god.configs.utils import (
    SYSTEM_CONFIG,
    USER_CONFIG,
    ConfigLevel,
    parse_dot_notation_to_dict,
)
from god.core.common import get_base_dir
from god.plugins.base import plugin_endpoints


def get_config_path(
    plugin: str, level: Union[str, ConfigLevel], base_dir: str = None
) -> str:
    """Get the path to plugin config, depending on the level

    Args:
        plugin: the plugin name
        level: the level that belong to config

    Returns:
        The path to config file

    Raises:
        AttributeError: when level is unknown
    """
    if level == ConfigLevel.SYSTEM:
        return SYSTEM_CONFIG

    if level == ConfigLevel.USER:
        return USER_CONFIG

    endpoints = plugin_endpoints(name="configs", base_dir=base_dir)
    if level == ConfigLevel.SHARED:
        return str(Path(endpoints["tracks"], plugin))

    if level == ConfigLevel.LOCAL:
        return str(Path(endpoints["untracks"], plugin))

    raise AttributeError(f'Unknown config level "{level}"')


def read_config_file(config_path: Union[str, Path]) -> Settings:
    """Read values from local config into dictionary

    # Args:
        config_path: path to config path

    # Returns:
        The setting object
    """
    settings = Settings()
    if Path(config_path).is_file():
        settings.set_values_from_yaml(config_path)

    return settings


def get_config(plugin: str = "configs") -> Settings:
    """Get the config information of a particular plugin

    Args:
        plugin: the name of the plugin (default the "configs" plugin)

    Returns:
        The Settings object of that config
    """
    base_setting = Settings()

    system_setting = read_config_file(SYSTEM_CONFIG)
    if system_setting.get(plugin, None) is not None:
        base_setting += system_setting[plugin]

    user_setting = read_config_file(USER_CONFIG)
    if user_setting.get(plugin, None) is not None:
        base_setting += user_setting[plugin]

    base_setting += read_config_file(get_config_path(plugin, ConfigLevel.SHARED))
    base_setting += read_config_file(get_config_path(plugin, ConfigLevel.LOCAL))

    return base_setting


def get_config_at_specific_level(
    plugin: str, level: Union[str, ConfigLevel], base_dir: str = None
) -> Settings:
    """Get the config of a plugin at a specific level

    Args:
        plugin: the plugin name
        level: the level that belong to config

    Returns:
        The settings module of that config

    """
    config_path = get_config_path(plugin, level, base_dir=base_dir)
    config = read_config_file(config_path)

    if level in [ConfigLevel.SYSTEM, ConfigLevel.USER]:
        return config.get(plugin, Settings())

    return config


def update_config(plugin: str, level: str, config_dict: Dict, base_dir: str = None):
    """Write the config out to YAML file

    # Args:
        plugin: the name of the plugin
        level: the config level of the plugin we wish to update
        config_dict: the configuration
    """
    config_path = get_config_path(plugin, level, base_dir=base_dir)
    config = read_config_file(config_path)

    # get the newly updated values
    parsed_values = {}
    for key, value in config_dict.items():
        parsed_values.update(parse_dot_notation_to_dict(key, value))

    if level in [ConfigLevel.SYSTEM, ConfigLevel.USER]:
        parsed_values = {plugin: parsed_values}

    config.set_values(**parsed_values)
    config.save(config_path)


def edit_config_file(plugin: str, level: str, base_dir: str = None):
    """Edit file, copy to temporary location and then move back

    # Args:
        plugin: the name of the plugin
        level: the config level of the plugin we wish to update
    """
    import shutil
    import subprocess
    import uuid

    config_path = get_config_path(plugin, level, base_dir=base_dir)

    temp = Path(get_base_dir(), ".god", "temp", f"{uuid.uuid1().hex}")
    if not temp.parent.exists():
        temp.parent.mkdir(parents=True)

    if Path(config_path).is_file():
        shutil.copy(config_path, str(temp))

    editor_cmd = get_config(plugin="configs").get("editor", ["vim"])
    if not isinstance(editor_cmd, (tuple, list)):
        print(f"WARN: Editor config should be in list format. Current: {editor_cmd}")
        editor_cmd = ["vim"]
    subprocess.run(list(editor_cmd) + [str(temp)])
    shutil.copy(temp, config_path)

    Path(temp).unlink()
