import shutil
import subprocess
import uuid
from pathlib import Path

from god.configs.base import Settings
from god.core.common import get_base_dir


def edit_file(file_path: str):
    """Edit file, copy to temporary location and then move back"""
    filename = Path(file_path).name
    temp = Path(get_base_dir(), ".god", "temp", f"{filename}_{uuid.uuid1().hex}")
    if not temp.parent.exists():
        temp.mkdir(parents=True)
    if Path(file_path).is_file():
        shutil.copy(file_path, str(temp))

    subprocess.run(["vim", temp])
    shutil.copy(temp, file_path)
    Path(temp).unlink()


def get_config(
    plugin: str, shared_tree: bool = True, local_tree: bool = True
) -> Settings:
    """Get the config information of a particular plugin

    Args:
        plugin: the name of the plugin
        shared_tree: whether to retrieve the config values in shared tree
        local_tree: whether to retrieve the config values in local tree

    Returns:
        The Settings object of that config
    """
    # @PRIORITY4: figure out shared_path and local_path the clean way
    shared_path = Path(get_base_dir(), ".god", "workings", "configs", "tracks")
    local_path = Path(get_base_dir(), ".god", "workings", "configs")
    base_setting = Settings()
    local_setting, shared_setting = Settings(), Settings()

    if (shared_path / "plugins" / plugin).is_file():
        shared_setting.set_values_from_yaml(shared_path / "plugins" / plugin)
    if (local_path / "plugins" / plugin).is_file():
        local_setting.set_values_from_yaml(local_path / "plugins" / plugin)

    if shared_tree:
        base_setting += shared_setting
    if local_tree:
        base_setting += local_setting

    return base_setting
