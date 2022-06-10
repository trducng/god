from abc import ABCMeta, abstractmethod
from typing import List

import god.storage.constants as c


class BaseStorage(metaclass=ABCMeta):
    """Base storage class to store objects

    # TODO: this is where we want to have filesystem interface
    Refer: https://filesystem-spec.readthedocs.io/en/latest/features.html
    """

    OBJECTS_PREFIX = c.DIR_OBJECTS
    DIRS_PREFIX = c.DIR_DIRS
    COMMITS_PREFIX = c.DIR_COMMITS

    @abstractmethod
    def __init__(self, config: str):
        pass

    @abstractmethod
    def _hash_path(self, hash_value: str, prefix: str = "") -> str:
        """Get the hash to path

        Args:
            hash_value: the hash of the object to be retrieved
            prefix: any prefix to attach before hash_value
        """
        raise NotImplementedError("Should implement `_hash_path`")

    @abstractmethod
    def _get(self, storage_paths: List[str], paths: List[str]):
        raise NotImplementedError("Should implement `_get`")

    @abstractmethod
    def _store(self, storage_paths: List[str], paths: List[str]):
        raise NotImplementedError("Should implement `_store`")

    @abstractmethod
    def _delete(self, storage_paths: List[str]):
        raise NotImplementedError("Should implement `_delete`")

    @abstractmethod
    def _have(self, storage_paths: List[str]) -> List[bool]:
        raise NotImplementedError("Should implement `_have`")

    @abstractmethod
    def _list(self, storage_prefix: str) -> List[str]:
        raise NotImplementedError("Should implement `_list`")

    ### objects
    def get_objects(self, hash_values: List[str], paths: List[str]):
        """Get the objects to a local file

        Args:
            hash_values: list of object hashes we wish to get
            paths: corresponding target locations that store the objects
        """
        sources = [
            self._hash_path(each, prefix=self.OBJECTS_PREFIX) for each in hash_values
        ]
        return self._get(storage_paths=sources, paths=paths)

    def store_objects(self, paths: List[str], hash_values: List[str]):
        """Store local object to storage

        Args:
            paths: corresponding target locations that store the objects
            hash_values: list of object hashes we wish to store
        """
        targets = [
            self._hash_path(each, prefix=self.OBJECTS_PREFIX) for each in hash_values
        ]
        return self._store(storage_paths=targets, paths=paths)

    def delete_objects(self, hash_values: List[str]):
        """Delete the objects with specified hash values from storage

        Args:
            hash_values: list of object hashes we wish to delete
        """
        targets = [
            self._hash_path(each, prefix=self.OBJECTS_PREFIX) for each in hash_values
        ]
        return self._delete(storage_paths=targets)

    def have_objects(self, hash_values: List[str]) -> List[bool]:
        """Check if objects with specified hash values exist

        Args:
            hash_values: list of object hashes we wish to check
        """
        targets = [
            self._hash_path(each, prefix=self.OBJECTS_PREFIX) for each in hash_values
        ]
        return self._have(storage_paths=targets)

    def list_objects(self) -> List[str]:
        """List objects, in the path format"""
        return self._list(
            storage_prefix=self._hash_path("", prefix=self.OBJECTS_PREFIX)
        )

    ### dirs
    def get_dirs(self, hash_values: List[str], paths: List[str]):
        """Get the directory to local

        Args:
            hash_values: list of directory hashes we wish to get
            paths: corresponding target locations that store the directory files
        """
        sources = [
            self._hash_path(each, prefix=self.DIRS_PREFIX) for each in hash_values
        ]
        return self._get(storage_paths=sources, paths=paths)

    def store_dirs(self, paths: List[str], hash_values: List[str]):
        """Store local directory to storage

        Args:
            paths: corresponding target locations that store the directores
            hash_values: list of directory hashes we wish to store
        """
        targets = [
            self._hash_path(each, prefix=self.DIRS_PREFIX) for each in hash_values
        ]
        return self._store(storage_paths=targets, paths=paths)

    def delete_dirs(self, hash_values: List[str]):
        """Delete the directories with specified hash values from storage

        Args:
            hash_values: list of directory hashes we wish to delete
        """
        targets = [
            self._hash_path(each, prefix=self.DIRS_PREFIX) for each in hash_values
        ]
        return self._delete(storage_paths=targets)

    def have_dirs(self, hash_values: List[str]) -> List[bool]:
        """Check if directories with specified hash values exist

        Args:
            hash_values: list of directory hashes we wish to check
        """
        targets = [
            self._hash_path(each, prefix=self.DIRS_PREFIX) for each in hash_values
        ]
        return self._have(storage_paths=targets)

    def list_dirs(self) -> List[str]:
        """List directories, in the path format"""
        return self._list(storage_prefix=self._hash_path("", prefix=self.DIRS_PREFIX))

    ### commits
    def get_commits(self, hash_values: List[str], paths: List[str]):
        """Get the commits to local

        Args:
            hash_values: list of commit hashes we wish to get
            paths: corresponding target locations that store the commit files
        """
        sources = [
            self._hash_path(each, prefix=self.COMMITS_PREFIX) for each in hash_values
        ]
        return self._get(storage_paths=sources, paths=paths)

    def store_commits(self, paths: List[str], hash_values: List[str]):
        """Store local commit to storage

        Args:
            paths: corresponding target locations that store the commits
            hash_values: list of commit hashes we wish to store
        """
        targets = [
            self._hash_path(each, prefix=self.COMMITS_PREFIX) for each in hash_values
        ]
        return self._store(storage_paths=targets, paths=paths)

    def delete_commits(self, hash_values: List[str]):
        """Delete the commits with specified hash values from storage

        Args:
            hash_values: list of commit hashes we wish to delete
        """
        targets = [
            self._hash_path(each, prefix=self.COMMITS_PREFIX) for each in hash_values
        ]
        return self._delete(storage_paths=targets)

    def have_commits(self, hash_values: List[str]) -> List[bool]:
        """Check if commits with specified hash values exist

        Args:
            hash_values: list of commit hashes we wish to check
        """
        targets = [
            self._hash_path(each, prefix=self.COMMITS_PREFIX) for each in hash_values
        ]
        return self._have(storage_paths=targets)

    def list_commits(self) -> List[str]:
        """List commits, in the path format"""
        return self._list(
            storage_prefix=self._hash_path("", prefix=self.COMMITS_PREFIX)
        )


class RemoteRefsMixin(metaclass=ABCMeta):
    """Mixins to get the refs"""

    REMOTE_REFS_PREFIX = "refs"

    @abstractmethod
    def _ref_path(self, ref: str, prefix: str = "") -> str:
        raise NotImplementedError("RemoteRefsMixin must implement `_ref_path`")

    def get_refs(self, refs: List[str], paths: List[str]):
        """Get the refs to local

        Args:
            refs: the name of the refs we wish to get
            paths: corresponding target locations that store the ref files
        """
        sources = [
            self._ref_path(each, prefix=self.REMOTE_REFS_PREFIX) for each in refs
        ]
        return self._get(storage_paths=sources, paths=paths)

    def store_refs(self, paths: List[str], refs: List[str]):
        """Store the refs from local to central storage

        Args:
            paths: corresponding target locations that store the local ref
            refs: the name of the remote refs we wish to upload
        """
        targets = [
            self._ref_path(each, prefix=self.REMOTE_REFS_PREFIX) for each in refs
        ]
        return self._store(storage_paths=targets, paths=paths)

    def delete_refs(self, refs: List[str]):
        """Delete the refs with specified name

        Args:
            refs: the name of refs we wish to delete in remote
        """
        targets = [
            self._ref_path(each, prefix=self.REMOTE_REFS_PREFIX) for each in refs
        ]
        return self._delete(storage_paths=targets)

    def have_refs(self, refs: List[str]) -> List[bool]:
        """Check if refs with specified names exist

        Args:
            refs: the name of the refs we wish to check
        """
        targets = [
            self._ref_path(each, prefix=self.REMOTE_REFS_PREFIX) for each in refs
        ]
        return self._have(storage_paths=targets)

    def list_refs(self) -> List[str]:
        """List refs, in the path format"""
        return self._list(
            storage_prefix=self._ref_path("", prefix=self.REMOTE_REFS_PREFIX)
        )
