from pathlib import Path
from typing import Union


def get_ref(ref: str, ref_dir: Union[str, Path]) -> str:
    """Get current commit

    Args:
        ref: the reference name
        ref_dir: the absolute path to reference folder

    Returns:
        The commit id
    """
    ref_path = Path(ref_dir, ref)
    if ref_path.is_file():
        with ref_path.open("r") as f_in:
            return f_in.read().strip()

    return ""


def update_ref(ref, commit_id, ref_dir):
    """Update reference value

    # Args:
        ref <str>: the reference name
        commit_id <str>: the commit id
        ref_dir <str>: the absolute path to reference folder
    """
    ref = Path(ref_dir, ref)
    with ref.open("w") as f_out:
        f_out.write(commit_id.strip())


def is_ref(ref, ref_dir):
    """Check if ref `ref` exists

    # Args:
        ref <str>: the reference name
        ref_dir <str>: the absolute path to reference folder
    """
    return Path(ref_dir, ref).is_file()
