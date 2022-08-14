from typing import Dict, List

from god.index.trackchanges import track_files
from god.plugins.base import load_manifest, plugin_endpoints
from god.utils.process import communicate


def _status(fds: List[str], plugin: str, hooks: Dict[str, List[str]]):
    """Track statuses of the directories

    Args:
        fds <str>: the directory to add (absolute path)
        plugin <str>: the name of the plugin
        hooks: [hookname: [hook-cmd1, hook-cmd2]] where each hook-cmd is a list
    """
    # HOOK: prepare-fds
    prestatus = hooks.get("prestatus", [])
    if prestatus:
        fds = communicate(prestatus, fds)  # type: ignore

    endpoints = plugin_endpoints(plugin)
    output = track_files(fds, endpoints["index"], endpoints["tracks"])

    # HOOK: further clean up
    poststatus = hooks.get("poststatus", [])
    if poststatus:
        output = communicate(poststatus, output)

    return output


def status(fdss: List[List[str]], plugins: List[str]) -> Dict[str, List]:
    """Check status of specified paths for specified plugins

    Example:
        >> status(
        >>     fds=[["."], ["patha", "pathb", "pathc"]],
        >>     plugins=["plugin1", "plugin2"]
        >> )

    Args:
        fds: len(fds) == len(plugins). Each plugin can have 1 or more paths to check
        plugins: plugin names that we wish to check status

    Returns:
        A dictionary, where each key is a plugin name, and each value is the
        list of list of files:
            - stage_add: new files added to staging index
            - stage_update: existing files that are in staging index
            - stage_remove: files removed from staging index
            - add: files add to working directory (but not in index)
            - update: files updated in working directory (but not reflected in index)
            - remove: files removed from working directory (but not reflected in index)
            - change-timestamp: files that content stay the same but changed timestamp
            - unset_mhash: files that are reverted back to the index version
    """
    # 1. get all plugins that has status
    # 1a. get all plugins
    # 1b. filter out those that do not have status

    # 2. collect hooks or status command of each plugin
    hooks = {
        "files": {"poststatus": ["god", "files", "hook", "poststatus"]},
        "configs": {},
        "plugins": {},
    }
    for plugin in plugins:
        manifest = load_manifest(plugin)
        if "status" in manifest.get("commands", {}):
            hooks[plugin] = manifest["commands"]["status"]

    # 3. run status of each of the plugin
    result = {}
    for idx, plugin in enumerate(plugins):
        exe = hooks[plugin]
        if isinstance(exe, list):
            result[plugin] = communicate(exe, fdss[idx])
        elif isinstance(exe, dict):
            result[plugin] = _status(fdss[idx], plugin, exe)
        else:
            print(f"[{plugin}] Unknown status hook: {exe}")

    return result
