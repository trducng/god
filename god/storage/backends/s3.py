import posixpath
from pathlib import Path
from typing import List

import boto3
from botocore.errorfactory import ClientError

from god.storage.backends.base import BaseStorage

DEFAULT_PATH = "storage"
DEFAULT_DIR_LEVEL = 2
BUCKET = ""
PREFIX = ""


class S3Storage(BaseStorage):
    """Store objects locally"""

    def __init__(self, config):
        # TODO: decide the format for storage config
        # TODO: might only allow relative path (to avoid overwrite hacking)
        # self._bucket = config["BUCKET"]
        # @PRIORITY2: remove these default values, raise error if users do not
        # supply these values
        self._bucket = config.get("BUCKET", "god-test-storage")
        self._prefix = config.get("PREFIX", "")
        self._dir_levels = config.get("DIR_LEVEL", DEFAULT_DIR_LEVEL)

    def _get_hash_path(self, hash_value: str) -> str:
        """From hash value, get relative hash path"""
        components = [
            hash_value[idx * 2 : (idx + 1) * 2] for idx in range(self._dir_levels)
        ]
        return posixpath.join(*components, hash_value[self._dir_levels * 2 :])

    def get_files(self, hash_values: List[str], file_paths: List[str]):
        """Get the file and store that file in file_path

        Args:
            hash_value: the object hash value
            file_path: the file path to copy to
        """
        s3r = boto3.resource("s3")
        # @PRIORITY3: use multiprocessing to speed up parallel downloads
        for each_hash, each_path in zip(hash_values, file_paths):
            Path(each_path).parent.mkdir(parents=True, exist_ok=True)

            s3r.Object(self._bucket, self._get_hash_path(each_hash)).download_file(
                each_path
            )

    def store_files(self, file_paths: List[str], hash_values: List[str]):
        """Store a file with a specific hash value

        Args:
            file_path: the file path
            hash_value: the hash value of the file
        """
        # PRIORITY3: make use of multiprocessing
        client = boto3.session.Session().client("s3")
        for each_hash, each_file in zip(hash_values, file_paths):
            client.upload_file(each_file, self._bucket, self._get_hash_path(each_hash))

    def deletes(self, hash_values: List[str]):
        """Delete object that has specific hash value

        Args:
            hash_value: the hash value of the object
        """
        # PRIORITY3: make use of bulk delete
        s3r = boto3.resource("s3")
        for each_hash in hash_values:
            s3r.Object(self._bucket, self._get_hash_path(each_hash)).delete()

    def exists(self, hash_values: List[str]) -> List[bool]:
        """Check whether an object or a file with a specific hash value exist

        Args:
            hash_values: the list of file hash values

        Returns:
            True if the file exists, False otherwise
        """
        result = []
        s3c = boto3.client("s3")
        for hash_value in hash_values:
            try:
                s3c.head_object(
                    Bucket=self._bucket, Key=self._get_hash_path(hash_value)
                )
                result.append(True)
            except ClientError:
                result.append(False)

        return result
