from enum import Enum
from pathlib import Path

SYSTEM_CONFIG = str(Path("/etc", "godconfig"))
USER_CONFIG = str(Path.home() / ".godconfig")


class ConfigLevel(str, Enum):
    """All of the config level"""

    SYSTEM = "system"
    USER = "user"
    SHARED = "shared"
    LOCAL = "local"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


def parse_dot_notation_to_dict(notation, value):
    """Parse dot notation to dictionary

    Example:
        >> parse_dot_notation_to_dict('abc.xyz': 10)
        { "ABC": { "XYZ", 10 }}

    # Args:
        notation <str>: the dot notation
        value <Any>: value

    # Returns:
        <{}>: the parsed dictionary
    """
    components = notation.split(".")
    components = list(reversed(components))

    result = {components[0]: value}
    for item in components[1:]:
        result = {item: result}

    return result
