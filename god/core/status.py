import json
import subprocess
from typing import Dict, List


def _status(fds: List[str], index_name: str, hooks: Dict[str, List[str]]):
    """Track statuses of the directories

    Args:
        fds <str>: the directory to add (absolute path)
        index_name <str>: the name of the index
        hooks: [hookname: [hook-cmd1, hook-cmd2]] where each hook-cmd is a list
    """
    # HOOK: prepare-fds
    prestatus = hooks.get("prestatus", [])
    if prestatus:
        p = subprocess.Popen(
            prestatus,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        result, _ = p.communicate(input=json.dumps(fds).encode())
        fds = json.loads(result)

    p = subprocess.Popen(
        ["god-index", "track", index_name],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    result, _ = p.communicate(input=json.dumps(fds).encode())
    output = json.loads(result)

    # HOOK: further clean up
    poststatus = hooks.get("poststatus", [])
    if poststatus:
        p = subprocess.Popen(
            poststatus,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        result, _ = p.communicate(input=json.dumps(output).encode())
        output = json.loads(result)

    return output


def status(fds: List[str], plugins: List[str]):
    """Track status"""
    # 1. get all plugins that has status
    # 1a. get all plugins
    # 1b. filter out those that do not have status
    from god.plugins.manifest import load_manifest

    hooks = {}
    for plugin in plugins:
        if plugin in ["files", "configs", "plugins"]:
            hooks[plugin] = {}
            continue

        manifest = load_manifest(plugin)
        if "status" in manifest.get("commands", {}):
            hooks[plugin] = manifest["commands"]["status"]

    # 2. collect hooks or status command of each plugin

    # 3. run status of each of the command
    result = {}
    for plugin, exe in hooks.items():
        if isinstance(exe, list):
            p = subprocess.Popen(exe, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            out, _ = p.communicate(input=json.dumps(fds).encode())
            result[plugin] = json.loads(out)
        elif isinstance(exe, dict):
            result[plugin] = _status(fds, plugin, exe)
        else:
            print(f"[{plugin}] Unknown status hook: {exe}")

    # 4. return
    return result
