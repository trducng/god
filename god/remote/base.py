import json
import shutil
from pathlib import Path
from typing import Dict, Union


def set_default_remote(name: str, link_path: Union[str, Path]):
    """Set default remote"""
    with open(link_path, "r") as fi:
        data = json.load(fi)
        remotes = data.get("REMOTES", {})

    if name not in remotes:
        raise RuntimeError(f'Remote "{name}" does not exist')

    data["DEFAULT_REMOTE"] = name
    with open(link_path, "w") as fo:
        json.dump(data, fo)


def unset_default_remote(link_path: Union[str, Path]):
    """Unset default remote"""
    with open(link_path, "r") as fi:
        data = json.load(fi)

    data["DEFAULT_REMOTE"] = ""
    with open(link_path, "w") as fo:
        json.dump(data, fo)


def get_default_remote(link_path: Union[str, Path]) -> str:
    """Get the default remote"""
    with open(link_path, "r") as fi:
        data = json.load(fi)

    return data.get("DEFAULT_REMOTE", "")


def get_remote(link_path: Union[str, Path], name: str = "") -> Dict[str, str]:
    """Get registered remote repository"""
    with open(link_path, "r") as fi:
        remotes = json.load(fi).get("REMOTES", {})

    if not name:
        return remotes

    if name not in remotes:
        raise RuntimeError(f'Remote "{name}" does not exist')

    return {name: remotes[name]}


def set_remote(
    name: str,
    location: str,
    link_path: Union[str, Path],
    ref_remotes_dir: Union[str, Path],
):
    """Add new remote to track"""
    if not name:
        raise AttributeError("Name must not be empty")
    if not location:
        raise AttributeError("Location must not be empty")

    with open(link_path, "r") as fi:
        data = json.load(fi)
        remotes = data.get("REMOTES", {})

    remotes[name] = location
    data["REMOTES"] = remotes
    with open(link_path, "w") as fo:
        json.dump(data, fo)

    remote_dir = Path(ref_remotes_dir, name)
    if remote_dir.is_file():
        remote_dir.unlink()
    remote_dir.mkdir(parents=True, exist_ok=True)


def unset_remote(
    name: str, link_path: Union[str, Path], ref_remotes_dir: Union[str, Path]
):
    """Delete tracked remote from local"""
    with open(link_path, "r") as fi:
        data = json.load(fi)
        remotes = data.get("REMOTES", {})

    if name not in remotes:
        raise RuntimeError(f'Remote "{name}" does not exist')

    remotes.pop(name)
    data["REMOTES"] = remotes
    with open(link_path, "w") as fo:
        json.dump(data, fo)

    remote_dir = Path(ref_remotes_dir, name)
    if remote_dir.exists():
        shutil.rmtree(remote_dir)
