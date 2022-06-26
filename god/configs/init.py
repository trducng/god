from pathlib import Path
from typing import Union

import yaml


def construct_empty_config(path: Union[str, Path], force: bool) -> None:
    """Construct empty config"""
    if Path(path).exists() and not force:
        raise ValueError(f"{path} already exists. Supply `force` to override")

    with open(path, "w") as fo:
        yaml.safe_dump({}, fo)


def init(working_dir: str, force: bool):
    """Setup configurations.

    Run this command to create:
        - untracked personal configuration
        - tracked shared configuration
    """
    # untracked
    Path(working_dir, "untracks").mkdir(exist_ok=force, parents=True)

    # tracked
    Path(working_dir, "tracks").mkdir(exist_ok=force, parents=True)
