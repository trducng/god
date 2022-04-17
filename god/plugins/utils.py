from pathlib import Path
from typing import Dict, List

import god.utils.constants as c
from god.core.common import get_base_dir


def plugin_endpoints(name: str) -> Dict[str, str]:
    """Get plugin index-path, track-path, untrack-path, cache-path

    Returns:
        [str]: index path
        [str]: track directory
        [str]: untrack directory
        [str]: cache directory
        [str]: the base directory
    """
    base_dir = Path(get_base_dir())
    result = {
        "index": str(base_dir / c.DIR_INDICES / name),
        "tracks": str(base_dir / c.DIR_HIDDEN_WORKING / name / "tracks"),
        "untracks": str(base_dir / c.DIR_HIDDEN_WORKING / name / "untracks"),
        "cache": str(base_dir / c.DIR_CACHE / name),
        "base_dir": str(base_dir),
    }

    if name == "files":
        result["tracks"] = str(base_dir)
        result["untracks"] = str(base_dir)

    return result


def installed_plugins() -> List[str]:
    """List the name of all installed plugins

    Returns:
        List of names of installed plugins
    """
    names = []

    plugin_dir = Path(plugin_endpoints("plugins")["tracks"])
    for each in sorted(plugin_dir.glob("*")):
        names.append(each.name)

    return names
