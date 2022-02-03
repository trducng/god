class BaseCache:
    """Base memory class"""

    def __init__(self, config):
        self._config = config
        pass

    def get_memory_location(self):
        """Get memory location"""
        pass

    def clean(self):
        """Clean the memory location"""
        pass

    def read(self):
        pass

    def write(self):
        pass

    def delete(self):
        pass

    def exists(self):
        pass
