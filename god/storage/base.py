class BaseStorage:
    """Base storage class to store objects

    # TODO: this is where we want to have filesystem interface
    Refer: https://filesystem-spec.readthedocs.io/en/latest/features.html
    """

    def __init__(self, config):
        pass

    def get_file(self, hash_value: str, file_path: str):
        raise NotImplementedError("Should implement `get_file`")

    def get_object(self, hash_value: str) -> bytes:
        raise NotImplementedError("Should implement `get_object`")

    def store_file(self, file_path: str, hash_value: str):
        raise NotImplementedError("Should implement `store_file`")

    def store_object(self, obj: bytes, hash_value: str):
        raise NotImplementedError("Should implement `store_object`")

    def delete(self, hash_value: str):
        raise NotImplementedError("Should implement `delete`")

    def exists(self, hash_value: str) -> bool:
        raise NotImplementedError("Should implement `exists`")
