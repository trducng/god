from god.core.index import Index


def status(index_path: str) -> tuple:
    """Check the status of records

    Given that the records config are not changed, this command check for:

    Args:
        index_path: the path to index file

    Returns:
        []: stage add - list of records are added in staging
        []: stage update - list of records are updated in staging
        []: stage remove - list of records are removed in staging
        []: add - list of records that are added in working dir but not staged
        []: update - list of records that are updated in working dir but not staged
    """
    with Index(index_path) as index:
        records = index.get_records()

    stage_add, stage_update, stage_remove = [], [], []
    add, update = [], []

    # staging
    for rn, rh, rmh, rwh, rm in records:
        if rm:
            stage_remove.append(rn)
            continue
        if not rh and rmh:
            stage_add.append(rn)
            continue
        if rmh and rmh != rh:
            stage_update.append(rn)

    # working
    for rn, rh, rmh, rwh, rm in records:
        if rm:
            continue
        if not rh and not rmh and rwh:
            add.append(rn)
            continue
        if rmh and rwh != rmh:
            update.append(rn)
            continue
        if not rmh and rwh != rh:
            update.append(rn)

    return stage_add, stage_update, stage_remove, add, update
