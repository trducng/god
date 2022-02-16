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
import subprocess
from pathlib import Path

from tqdm import tqdm

from god.branches.trackchanges import track_working_changes
from god.core.index import Index
from god.files.descriptors import FileDescriptor


def add(fds, index_path, dir_obj, base_dir, dir_cache_records, dir_records):
    """Add the files, directories & all records to staging area.

    Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dir_obj <str>: the path to object directory
        base_dir <str>: project base directory
        dir_cache_records <str>: directory containing working records
        dir_records <str>: directory containing to-be-committed records
    """
    add, update, remove, reset_tst, unset_mhash = track_working_changes(
        fds, index_path, base_dir
    )
    # @TODO: hook1: track-working changes -> might need hook here
    # seems to hook to clean up the variables `add`, `update`,...
    # decide the config format (should be YAML like)

    # @TODO: move files to cache, create symlink
    symlink = True
    if symlink:
        print("Move files to cache and create symlink")

    # @TODO: parse the plugins from settings
    plugins = [["god-compress"], ["god-encrypt"]]
    # @TODO: handle files with the same hash (maybe add the fp here)
    for fp, fh, _ in tqdm(add + update):
        descriptor = FileDescriptor.descriptor()
        descriptor["hash"] = "sha256"
        descriptor["checksum"] = fh

        # process each files with plugin
        for plugin in plugins:
            # @NOTE: declare the plugin inside config, and call here
            p = subprocess.run(plugin + [fp])
            if p.returncode:
                raise RuntimeError(f"Error when running plugin {plugin}")
            if p.stdout.strip():
                result = json.loads(p.stdout.strip())
                if "fp" in result:
                    fp = result["fp"]
                if "fh" in result:
                    fh = result["fh"]
                if "plugin" in result:
                    descriptor["plugin"].append(result["plugin"])

        # @TODO: upload the files to storage
        # @TODO: suppose that we get the storage implementation from config, but we
        # should get this knowledge from some place like plugins manager and config
        # @TODO: starting a new process for each file in a for loop is not quite nice
        # because of the overhead in initiation process (e.g. reading the config,
        # finding the base...). Maybe we can have an option for inter-process
        # communication (https://en.wikipedia.org/wiki/Inter-process_communication)
        # with MPI, shared memory, or etc. There is a nice Python code snippet here:
        # https://stackoverflow.com/questions/9743838/python-subprocess-in-parallel
        # @NOTE: at the moment, still use this slow approach because it's simpler, we
        # can experiment the modes of inter-process communicatin later.
        # @TODO: nevertheless, if we want to simplify the plugin creation process for
        # developers, we need to implement inter-process communication strategy that
        # they can just reuse (maybe setup a shared memory space first hand?).
        descriptor["location"] = fh
        p = subprocess.run(["god-storage-s3", "store-file", fp, fh])
        if p.returncode:
            raise RuntimeError(f"Error during adding file: {p.stderr}")

        # @TODO: save descriptor
        p = subprocess.run(
            ["god-descriptor", "store-descriptor", json.dumps(descriptor)]
        )
        if p.returncode:
            raise RuntimeError(f"Error during saving descriptor: {p.stderr}")

        # @TODO: delete the local storage files to save space
        if descriptor["checksum"] != descriptor["location"]:
            Path(fp).unlink()

    # @TODO: hook2: before update index

    # update the index
    with Index(index_path) as index:
        # @TODO: must expose index as command so that 3rd-party plugins can make use
        # of it

        # update files
        index.update(
            add=add,
            update=update,
            remove=remove,
            reset_tst=reset_tst,
            unset_mhash=unset_mhash,
        )

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
