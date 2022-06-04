"""Prepare repository for commit"""
import json
import logging
import subprocess
from pathlib import Path
from typing import Dict, List

from god.core.files import resolve_paths
from god.utils.process import communicate


def _add(fds: List[str], base_dir: str, index_name: str, hooks: Dict[str, List[str]]):
    """Add the files, directories & all records to staging area.

    Args:
        fds <list str>: the directory to add (absolute path)
        base_dir <str>: project base directory
        index_name <str>: the name of the index
    """
    # HOOK: ADD-PRE-RUN
    preadd = hooks.get("preadd", [])
    if preadd:
        fds = communicate(command=preadd, stdin=fds)  # type: ignore

    p = subprocess.Popen(
        ["god-index", "track", index_name, "--working"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )

    out, _ = p.communicate(input=json.dumps(fds).encode())
    add, update, remove, reset_tst, unset_mhash = json.loads(out)

    # HOOK: ADD-POST-TRACK
    # @TODO: hook1: track-working changes -> might need hook here
    # seems to hook to clean up the variables `add`, `update`,...
    # decide the config format (should be YAML like)

    # each item in new_objs has format [prefix, hash, path]
    add_ = [[fp, str(Path(base_dir, fp))] for fp, _, _ in add]
    update_ = [[fp, str(Path(base_dir, fp))] for fp, _, _ in update]

    # @TODO: parse the plugins from settings, maybe also parsing the args
    # plugins = [["god-compress"], ["god-encrypt"]]
    plugins = []
    for plugin in plugins:  # get plugin params, and skip
        logging.info(f"Running plugin {plugin}")
        child = subprocess.Popen(
            args=plugin,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )
        output, _ = child.communicate(input=json.dumps(add_).encode())
        if child.returncode:
            # @TODO: note in doc, printing diagnostic message is the role of the child
            # process, not the role of parent process
            raise RuntimeError(f"{plugin} exit with status {child.returncode}")
        add_ = json.loads(output.strip())

        child = subprocess.Popen(
            args=plugin,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )
        output, _ = child.communicate(input=json.dumps(update_).encode())
        if child.returncode:
            # @TODO: note in doc, printing diagnostic message is the role of the child
            # process, not the role of parent process
            raise RuntimeError(f"{plugin} exit with status {child.returncode}")
        update_ = json.loads(output.strip())

    # construct add and update (this relates to `plugins`)
    new_objs = []
    if plugins:
        # assume that add_ and update_ will change
        for idx, (_, path) in enumerate(add_):
            new_objs.append([path, add[idx][-1]])
        for idx, (_, path) in enumerate(update_):
            new_objs.append([path, update[idx][-1]])
    else:
        for idx, item in enumerate(add):
            new_objs.append([add_[idx][1], item[1]])
        for idx, item in enumerate(update):
            new_objs.append([update_[idx][1], item[1]])

    # new_objs = [[e[1], e[0]] for e in add_ + update_]     # [path, hash]

    # move the objects to storage
    # @TODO: suppose that we get the storage implementation from config, but we
    # should get this knowledge from some place like plugins manager and config
    storage_cmd = ["god", "storages"]
    child = subprocess.Popen(
        args=storage_cmd + ["store-objects"],
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
    )
    print(new_objs)
    _, _ = child.communicate(input=json.dumps(new_objs).encode())
    if child.returncode:
        raise RuntimeError(f"Cannot run {storage_cmd}")

    # @TODO: remove cache

    # update the index
    if unset_mhash:
        p = subprocess.Popen(
            ["god-index", "revert", index_name, "--mhash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(unset_mhash).encode())

    if reset_tst:
        p = subprocess.Popen(
            ["god-index", "revert", index_name],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(reset_tst).encode())

    if remove:
        p = subprocess.Popen(
            ["god-index", "delete", index_name, "--staged"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(remove).encode())

    if update:
        p = subprocess.Popen(
            ["god-index", "update", index_name],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(update).encode())

    if add:
        p = subprocess.Popen(
            ["god-index", "add", index_name, "--staged"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _, _ = p.communicate(input=json.dumps(add).encode())

    # @TODO: hook3: after update index


def add(fds, plugin):
    """Add the files, directories & all records to staging area.

    Args:
        fds <str>: the directory or files to add (relative path to base_dir)
        plugin <str>: the plugin to add
    """
    # @TODO: should we supply the settings, or should we let the plugins figure out
    # the settings values?
    #   - It seems we have to expose the config as a component, like a plumbing-command
    # so that 3rd-party plugin can readily use it to extract information they need.
    from god.core.common import get_base_dir
    from god.plugins.manifest import load_manifest

    if plugin == "files":
        base_dir = get_base_dir()
        if not fds:
            fds = ["."]
        fds = [str(Path(_).resolve()) for _ in fds]
        fds = resolve_paths(fds, base_dir)
        hooks = {}
    else:
        base_dir = str(Path(get_base_dir(), ".god", "workings", plugin, "tracks"))
        hooks = load_manifest(plugin).get("commands", {}).get("add", {})

    _add(fds, base_dir, plugin, hooks)
