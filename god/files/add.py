"""Prepare repository for commit

Behaviors:
    - Add files to staging area
    - Construct new record collections -> maybe need interactive
    - Delete record collections -> maybe need interactive
    - Update record collections -> maybe need interactive
    - Add/Update/Remove records to/from record collection
    -> We need to understand about the working area for records
"""
import json
import logging
import shutil
import subprocess
from pathlib import Path

from god.core.files import resolve_paths
from god.files.descriptors import FileDescriptor


def add(fds, base_dir):
    """Add the files, directories & all records to staging area.

    Args:
        fds <list str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dir_obj <str>: the path to object directory
        base_dir <str>: project base directory
        dir_cache_records <str>: directory containing working records
        dir_records <str>: directory containing to-be-committed records
    """
    fds = resolve_paths(fds, base_dir)
    p = subprocess.Popen(
        ["god-index", "track", "files", "--working"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    out, _ = p.communicate(input=json.dumps(fds).encode())
    add, update, remove, reset_tst, unset_mhash = json.loads(out)

    # @TODO: hook1: track-working changes -> might need hook here
    # seems to hook to clean up the variables `add`, `update`,...
    # decide the config format (should be YAML like)

    # each item in new_objs has format [prefix, hash, path]
    new_objs = [[fp, fh, str(Path(base_dir, fp))] for fp, fh, _ in add] + [
        [fp, fh, str(Path(base_dir, fp))] for fp, fh, _ in update
    ]
    descriptors = []
    for _, fh, _ in new_objs:
        descriptor = FileDescriptor.descriptor()
        descriptor["hash"] = "sha256"
        descriptor["checksum"] = fh
        descriptors.append(descriptor)

    # @TODO: move files to cache, create symlink
    symlink = False
    if symlink:
        logging.info("Move files to cache and create symlink")
        cache_folder = Path(
            base_dir, ".god", "cache", "files-add"
        )  # @TODO: get from config
        cache_folder.mkdir(parents=True, exist_ok=True)
        for idx in range(len(new_objs)):
            target = cache_folder / new_objs[idx][1]
            if not target.exists():
                shutil.copy(new_objs[idx][2], target)
                target.chmod(0o440)
            new_objs[idx][2] = str(target)

    # @TODO: parse the plugins from settings, maybe also parsing the args
    plugins = [["god-compress"], ["god-encrypt"]]
    for plugin in plugins:  # get plugin params, and skip
        logging.info(f"Running plugin {plugin}")
        child = subprocess.Popen(
            args=plugin,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )
        output, _ = child.communicate(input=json.dumps(new_objs).encode())
        if child.returncode:
            # @TODO: note in doc, printing diagnostic message is the role of the child
            # process, not the role of parent process
            raise RuntimeError(f"{plugin} exit with status {child.returncode}")

        output = json.loads(output.strip())  # [(hash, path, plugin)]
        for idx in range(len(output)):
            new_objs[idx][1] = output[idx][0]
            new_objs[idx][2] = output[idx][1]
            descriptors[idx]["plugins"].append(output[idx][2])

    # finalize descriptor info
    for idx in range(len(new_objs)):
        descriptors[idx]["location"] = new_objs[idx][1]
        descriptors[idx]["plugins"] = list(reversed(descriptors[idx]["plugins"]))

    # move the objects to storage
    # @TODO: suppose that we get the storage implementation from config, but we
    # should get this knowledge from some place like plugins manager and config
    storage_cmd = "god-storage-s3"
    child = subprocess.Popen(
        args=[storage_cmd, "store-files"],
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
    )
    _, _ = child.communicate(
        input=json.dumps([[each[2], each[1]] for each in new_objs]).encode()
    )
    if child.returncode:
        raise RuntimeError(f"Cannot run {storage_cmd}")

    # store descriptor
    child = subprocess.Popen(
        args=["god-descriptor", "store-descriptors"],
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
    )
    output, _ = child.communicate(input=json.dumps(descriptors).encode())
    if child.returncode:
        raise RuntimeError("Cannot store descriptor")
    output = json.loads(output.strip())  # [(hash, path, plugin)]

    # @TODO: remove cache

    # @TODO: hook2: before update index

    # update the index
    if unset_mhash:
        p = subprocess.Popen(
            ["god-index", "revert", "files", "--mhash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(unset_mhash).encode())

    if reset_tst:
        p = subprocess.Popen(
            ["god-index", "revert", "files"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(reset_tst).encode())

    if remove:
        p = subprocess.Popen(
            ["god-index", "delete", "files", "--staged"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(remove).encode())

    if update:
        p = subprocess.Popen(
            ["god-index", "update", "files"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(update).encode())

    if add:
        p = subprocess.Popen(
            ["god-index", "add", "files", "--staged"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(add).encode())

    # @TODO: move this block to the record-plugin code
    # current_records = index.get_records()
    # records_update = []
    # for rn, rh, rmh, rwh, rm in current_records:
    #     if rwh == rmh:
    #         continue
    #     records_update.append((rn, rwh))
    #     copy_tree(rwh, dir_cache_records, dir_records)
    # index.update_records(update=records_update)

    # @TODO: hook3: after update index
