from god.configs.utils import get_config
from god.storage.backends.base import BaseStorage
from god.storage.backends.local import LocalStorage
from god.storage.backends.s3 import S3Storage

STORAGE = {
    "local": LocalStorage,
    "s3": S3Storage,
}


def get_backend(plugin: str) -> BaseStorage:
    """Get corresponding backend"""
    settings = get_config("storages")

    if plugin == "configs":
        return STORAGE["local"]({})

    default = settings.get("DEFAULT", {"STORAGE": "local"})
    plugin_storage = settings.get("PLUGINS", {}).get(plugin, default)

    return STORAGE[plugin_storage["STORAGE"]](plugin_storage)
