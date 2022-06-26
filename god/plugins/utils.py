from pathlib import Path
from typing import List

from god.core.common import plugin_endpoints


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
