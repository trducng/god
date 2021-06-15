from pathlib import Path

import yaml


def get_ref(ref, ref_dir):
    """Get current commit

    # Args:
        ref <str>: the reference name
        ref_dir <str>: the absolute path to reference folder

    # Returns:
        <str>: the commit id
    """
    ref = Path(ref_dir, ref)
    if ref.is_file():
        with ref.open("r") as f_in:
            ref = f_in.read().strip()
        return ref

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
