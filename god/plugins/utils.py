from pathlib import Path
from typing import Dict

import god.utils.constants as c
from god.core.common import get_base_dir


def plugin_endpoints(name: str) -> Dict[str, str]:
    """Get plugin index-path, track-path, untrack-path, cache-path

    Returns:
        [str]: index path
        [str]: track directory
        [str]: untrack directory
        [str]: cache directory
    """
    base_dir = Path(get_base_dir())

    return {
        "index": str(base_dir / c.DIR_INDICES / name),
        "tracks": str(base_dir / c.DIR_HIDDEN_WORKING / name / "tracks"),
        "untracks": str(base_dir / c.DIR_HIDDEN_WORKING / name / "untracks"),
        "cache": str(base_dir / c.DIR_CACHE / name),
        "base_dir": str(base_dir),
    }
