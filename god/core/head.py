import json
from pathlib import Path
from typing import Union


class HEAD(dict):
    """Represent the HEAD object"""

    _VALID_ENTRIES = ["REFS", "COMMITS", "EXPOSED_PLUGINS"]

    def _raise_invalid_key(self, key):
        if key not in self._VALID_ENTRIES:
            raise AttributeError(
                f'[Head] Unknown key "{key}". Accept: {self._VALID_ENTRIES}'
            )

    def __getitem__(self, key):
        """Get Head value. Valid Head are"""
        self._raise_invalid_key(key)
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        """Get Head value. Valid Head are"""
        self._raise_invalid_key(key)
        return super().__setitem__(key, value)

    def get(self, key, default):
        """Get Head value. Valid Head are"""
        self._raise_invalid_key(key)
        return super().get(key, default)

    def update(self, *args, **kwargs):
        """Update the head object"""
        for key, value in dict(*args, **kwargs).items():
            self[key] = value

    def commit(self) -> Union[str, None]:
        """Return commit information"""
        return self.get("COMMITS", None)

    def ref(self) -> Union[str, None]:
        """Return ref information"""
        return self.get("REFS", None)

    def exposed_plugin(self) -> Union[str, None]:
        """Return exposed plulgin information"""
        return self.get("EXPOSED_PLUGINS", None)


def read_HEAD(file_head: Union[str, Path]) -> HEAD:
    """Get current refs and snapshots from HEAD

    Args:
        file_head <str>: path to file head

    Returns:
        Head object
    """
    with open(file_head, "r") as f_in:
        config = json.load(f_in)

    return HEAD(config)


def update_HEAD(file_head: Union[str, Path], **kwargs):
    """Update HEAD reference

    # Args:
        file_head <str>: path to file head
        ref <str>: reference name
    """
    head_obj = read_HEAD(file_head)
    head_obj.update(kwargs)

    # remove unnecessary entries
    keys = list(head_obj.keys())
    for k in keys:
        if head_obj[k] is None:
            head_obj.pop(k)

    # write HEAD
    with open(file_head, "w") as f_out:
        json.dump(head_obj, f_out)
