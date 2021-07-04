import yaml


def read_HEAD(file_head):
    """Get current refs and snapshots from HEAD

    # Args:
        file_head <str>: path to file head

    # Returns:
        <str>: branch reference
        <str>: snapshot name
    """
    with open(file_head, "r") as f_in:
        config = yaml.safe_load(f_in)

    return (
        config.get("REFS", None),
        config.get("SNAPSHOTS", None),
        config.get("COMMITS", None),
    )


def update_HEAD(file_head, **kwargs):
    """Update HEAD reference

    # Args:
        file_head <str>: path to file head
        ref <str>: reference name
    """
    with open(file_head, "r") as f_in:
        config = yaml.safe_load(f_in)

    config.update(kwargs)

    # remove unnecessary entries
    keys = list(config.keys())
    for k in keys:
        if config[k] is None:
            config.pop(k)

    # write HEAD
    with open(file_head, "w") as f_out:
        yaml.safe_dump(config, f_out)
