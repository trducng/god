from abc import ABCMeta, abstractmethod
from typing import Dict, List


class BaseStorage(metaclass=ABCMeta):
    """Base storage class to store objects

    # TODO: this is where we want to have filesystem interface
    Refer: https://filesystem-spec.readthedocs.io/en/latest/features.html
    """

    @abstractmethod
    def __init__(self, config: Dict):
        pass

    @abstractmethod
    def get_files(self, hash_values: List[str], file_paths: List[str]):
        raise NotImplementedError("Should implement `get_files`")

    @abstractmethod
    def store_files(self, file_paths: List[str], hash_values: List[str]):
        raise NotImplementedError("Should implement `store_files`")

    @abstractmethod
    def deletes(self, hash_values: List[str]):
        raise NotImplementedError("Should implement `deletes`")

    @abstractmethod
    def exists(self, hash_values: List[str]) -> List[bool]:
        raise NotImplementedError("Should implement `exists`")
