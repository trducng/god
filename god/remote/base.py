import shutil
from pathlib import Path
from typing import Dict, Union

import yaml


def set_default_remote(name: str, remote_config_path: Union[str, Path]):
    """Set default remote"""
    with open(remote_config_path, "r") as fi:
        data = yaml.safe_load(fi)
        remotes = data.get("remotes", {})

    if name not in remotes:
        raise RuntimeError(f'Remote "{name}" does not exist')

    data["default_remote"] = name
    with open(remote_config_path, "w") as fo:
        yaml.dump(data, fo)


def unset_default_remote(remote_config_path: Union[str, Path]):
    """Unset default remote"""
    with open(remote_config_path, "r") as fi:
        data = yaml.safe_load(fi)

    data["default_remote"] = ""
    with open(remote_config_path, "w") as fo:
        yaml.dump(data, fo)


def get_default_remote(remote_config_path: Union[str, Path]) -> str:
    """Get the default remote"""
    with open(remote_config_path, "r") as fi:
        data = yaml.safe_load(fi)

    return data.get("default_remote", "")


def get_remote(remote_config_path: Union[str, Path], name: str = "") -> Dict[str, str]:
    """Get registered remote repository"""
    with open(remote_config_path, "r") as fi:
        remotes = yaml.safe_load(fi).get("remotes", {})

    if not name:
        return remotes

    if name not in remotes:
        raise RuntimeError(f'Remote "{name}" does not exist')

    return {name: remotes[name]}


def set_remote(
    name: str,
    location: str,
    remote_config_path: Union[str, Path],
    ref_remotes_dir: Union[str, Path],
):
    """Add new remote to track"""
    if not name:
        raise AttributeError("Name must not be empty")
    if not location:
        raise AttributeError("Location must not be empty")

    with open(remote_config_path, "r") as fi:
        data = yaml.safe_load(fi)
        remotes = data.get("remotes", {})

    remotes[name] = location
    data["remotes"] = remotes
    with open(remote_config_path, "w") as fo:
        yaml.dump(data, fo)

    remote_dir = Path(ref_remotes_dir, name)
    if remote_dir.is_file():
        remote_dir.unlink()
    remote_dir.mkdir(parents=True, exist_ok=True)


def unset_remote(
    name: str, remote_config_path: Union[str, Path], ref_remotes_dir: Union[str, Path]
):
    """Delete tracked remote from local"""
    with open(remote_config_path, "r") as fi:
        data = yaml.safe_load(fi)
        remotes = data.get("remotes", {})

    if name not in remotes:
        raise RuntimeError(f'Remote "{name}" does not exist')

    remotes.pop(name)
    data["remotes"] = remotes
    with open(remote_config_path, "w") as fo:
        yaml.dump(data, fo)

    remote_dir = Path(ref_remotes_dir, name)
    if remote_dir.exists():
        shutil.rmtree(remote_dir)
