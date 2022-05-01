from abc import ABCMeta, abstractmethod
from typing import List


class BaseStorage(metaclass=ABCMeta):
    """Base storage class to store objects

    # TODO: this is where we want to have filesystem interface
    Refer: https://filesystem-spec.readthedocs.io/en/latest/features.html
    """

    @abstractmethod
    def __init__(self, config: str):
        pass

    ### objects
    @abstractmethod
    def get_objects(self, hash_values: List[str], paths: List[str]):
        raise NotImplementedError("Should implement `get_objects`")

    @abstractmethod
    def store_objects(self, paths: List[str], hash_values: List[str]):
        raise NotImplementedError("Should implement `store_objects`")

    @abstractmethod
    def delete_objects(self, hash_values: List[str]):
        raise NotImplementedError("Should implement `delete_objects`")

    @abstractmethod
    def have_objects(self, hash_values: List[str]) -> List[bool]:
        raise NotImplementedError("Should implement `have_objects`")

    @abstractmethod
    def list_objects(self) -> List[str]:
        raise NotImplementedError("Should implement `list_objects`")

    ### dirs
    @abstractmethod
    def get_dirs(self, hash_values: List[str], paths: List[str]):
        raise NotImplementedError("Should implement `get_files`")

    @abstractmethod
    def store_dirs(self, paths: List[str], hash_values: List[str]):
        raise NotImplementedError("Should implement `store_files`")

    @abstractmethod
    def delete_dirs(self, hash_values: List[str]):
        raise NotImplementedError("Should implement `delete_dirs`")

    @abstractmethod
    def have_dirs(self, hash_values: List[str]) -> List[bool]:
        raise NotImplementedError("Should implement `have_dirs`")

    @abstractmethod
    def list_dirs(self) -> List[str]:
        raise NotImplementedError("Should implement `list_dirs`")

    ### commits
    @abstractmethod
    def get_commits(self, hash_values: List[str], paths: List[str]):
        raise NotImplementedError("Should implement `get_files`")

    @abstractmethod
    def store_commits(self, paths: List[str], hash_values: List[str]):
        raise NotImplementedError("Should implement `store_files`")

    @abstractmethod
    def delete_commits(self, hash_values: List[str]):
        raise NotImplementedError("Should implement `delete_commits`")

    @abstractmethod
    def have_commits(self, hash_values: List[str]) -> List[bool]:
        raise NotImplementedError("Should implement  have_commits`")

    @abstractmethod
    def list_commits(self) -> List[str]:
        raise NotImplementedError("Should implement `list_commits`")
