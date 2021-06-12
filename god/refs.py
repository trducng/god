from pathlib import Path

import yaml


def get_ref(refs, dir_refs):
    """Get current commit

    # Args:
        refs <str>: the reference name
        dir_refs <str>: the absolute path to reference folder

    # Returns:
        <str>: the commit id
    """
    refs = Path(dir_refs, refs)
    if refs.is_file():
        with refs.open("r") as f_in:
            refs = f_in.read()
        return refs

    return ""


def update_ref(refs, commit_id, dir_refs):
    """Update reference value

    # Args:
        refs <str>: the reference name
        commit_id <str>: the commit id
        dir_refs <str>: the absolute path to reference folder
    """
    refs = Path(dir_refs, refs)
    with refs.open("w") as f_out:
        f_out.write(commit_id)
