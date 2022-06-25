import hashlib
import shutil
from collections import defaultdict
from pathlib import Path

from god.configs.base import settings


def get_instances_from_snap(file_path):
    """Get instances from snap"""
    result = []
    with open(file_path, "r") as f_in:
        lines = f_in.read().splitlines()
        start_idx = 0
        for idx, each_line in enumerate(lines):
            if each_line == "=" * 88:
                start_idx = idx + 1

        for idx in range(start_idx, len(lines)):
            result.append(lines[idx].split(","))  # TODO: unsafe with quoted ,

    return result


def get_hashes(name, active=False):
    """Get the hashes of a file_path with name"""
    files = list(Path(settings.DIR_SNAP).glob("*"))
    names_hashes = defaultdict(list)
    for fn in files:
        components = fn.name.split("_")
        name_ = "_".join(components[:-1])
        hash_ = components[-1] if active else components[-1].replace("-", "")
        names_hashes[name_].append(hash_)

    return names_hashes.get(name, [])


def add(file_path, name, force=False):
    """Add snapshot to god repo"""

    # calculate sha256
    with open(file_path, "rb") as f_in:
        file_hash = hashlib.sha256(f_in.read()).hexdigest()

    # get hashes of the target snapshot name
    all_hashes = get_hashes(name, active=True)
    hashes = [each.replace("-", "") for each in all_hashes]
    active_hash = None
    for each in all_hashes:
        if "-" in each:
            active_hash = each.replace("-", "")

    # if there already exists
    if hashes:
        if file_hash in hashes:
            if active_hash == file_hash:
                print("Snapshot already active")
            else:
                print(f"Snapshot already active with hash {active_hash}")
            return

        if not force:
            print(
                f'Snapshot with name "{name}" already exists. Please pick different name'
            )
            return

    # perform copy
    shutil.copy(
        file_path, Path(settings.DIR_SNAP, f"{name}_{file_hash}-"), follow_symlinks=True
    )

    # reprioritize old one
    if active_hash:
        shutil.copy(
            Path(settings.DIR_SNAP, f"{name}_{active_hash}-"),
            Path(settings.DIR_SNAP, f"{name}_{active_hash}"),
        )

    return file_hash


def ls():
    files = list(Path(settings.DIR_SNAP).glob("*"))
    names = []
    for fn in files:
        components = fn.name.split("_")
        name_ = "_".join(components[:-1])
        names.append(name_)

    return sorted(list(set(names)))


def compare(fp1, fp2, compare_type=None):
    """Compare the 2 snapshots to check for difference

    @TODO: in case fp1 and fp2 have different columns, check for only overlapped
    columns.

    # Args
        fp1 <str>: path to snapshot 1
        fp2 <str>: path to snapshot 2
        compare_type <int>: comparision type

    # Returns
        <
    """
    content1 = get_instances_from_snap(fp1)
    content2 = get_instances_from_snap(fp2)
    content1_dict = {each[0]: each[1:] for each in content1[1:]}
    content2_dict = {each[0]: each[1:] for each in content2[1:]}
    content1_ids = set(content1_dict.keys())
    content2_ids = set(content2_dict.keys())

    # TODO check for similar index

    # TODO check for overlapping columns

    # check for added instances
    add_ids = list(content2_ids.difference(content1_ids))
    add = [content2[0]]
    for add_id in add_ids:
        add.append((add_id,) + tuple(content2_dict[add_id]))

    # check for removed instances
    remove_ids = list(content1_ids.difference(content2_ids))
    remove = [content1[0]]
    for remove_id in remove_ids:
        remove.append((remove_id,) + tuple(content1_dict[remove_id]))

    # check for updated instances
    remain_ids = list(content2_ids.intersection(content1_ids))
    update = [content1[0]]
    for remain_id in remain_ids:
        c1 = content1_dict[remain_id]
        c2 = content2_dict[remain_id]
        for c1_, c2_ in zip(c1, c2):
            if c1_ != c2_:
                update.append((((remain_id,) + tuple(c1)), ((remain_id,) + tuple(c2))))

    return add, remove, update


def refresh(file_path, config, db_name):
    """Refresh the snapshot to retrieve the most up-to-date information"""
    content = get_instances_from_snap(file_path)
    # content_ids = [each[0] for each in content[1:]]

    # retrieve all the instances in index

    # get removed ids

    # get updated information of remaining

    # update timestamp, maintain all other information

    return content


def validate_snap_format(file_path):
    """Validate if the snapshot has correct format

    Especially the header field.
    """
    pass
