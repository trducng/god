import posixpath
from pathlib import Path

import boto3
import click
from botocore.errorfactory import ClientError

from god.storage.base import BaseStorage

DEFAULT_PATH = "storage"
DEFAULT_DIR_LEVEL = 2


class S3Storage(BaseStorage):
    """Store objects locally"""

    def __init__(self, config):
        # TODO: decide the format for storage config
        # TODO: might only allow relative path (to avoid overwrite hacking)
        # self._bucket = config["BUCKET"]
        self._bucket = "god-test-storage"
        self._dir_levels = config.get("DIR_LEVEL", DEFAULT_DIR_LEVEL)

    def _get_hash_path(self, hash_value: str) -> str:
        """From hash value, get relative hash path"""
        components = [
            hash_value[idx * 2 : (idx + 1) * 2] for idx in range(self._dir_levels)
        ]
        return posixpath.join(*components, hash_value[self._dir_levels * 2 :])

    def get_file(self, hash_value: str, file_path: str):
        """Get the file and store in file_path

        Args:
            hash_value: the object hash value
            file_path: the file path to copy to
        """
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        s3r = boto3.resource("s3")
        s3r.Object(self._bucket, self._get_hash_path(hash_value)).download_file(
            file_path
        )

    def get_object(self, hash_value: str) -> bytes:
        """Get the file and store as bytes

        Args:
            hash_value: the object hash value

        Returns:
            the object bytes
        """
        raise NotImplementedError("cannot get_object in a plugin model")

    def store_file(self, file_path: str, hash_value: str):
        """Store a file with a specific hash value

        Args:
            file_path: the file path
            hash_value: the hash value of the file
        """
        s3_client = boto3.session.Session().client("s3")
        s3_client.upload_file(file_path, self._bucket, self._get_hash_path(hash_value))

    def store_object(self, obj: bytes, hash_value: str):
        """Store an object with a specific hash value

        Args:
            obj: the object to store
            file_path: the file path
        """
        raise NotImplementedError("cannot store_object in a plugin model")

    def delete(self, hash_value: str):
        """Delete object that has specific hash value

        Args:
            hash_value: the hash value of the object
        """
        s3r = boto3.resource("s3")
        s3r.Object(self._bucket, self._get_hash_path(hash_value)).delete()

    def exists(self, hash_value: str) -> bool:
        """Check whether an object or a file with a specific hash value exist

        Args:
            hash_value: the file hash value

        Returns:
            True if the file exists, False otherwise
        """
        s3c = boto3.client("s3")
        try:
            s3c.head_object(Bucket=self._bucket, Key=self._get_hash_path(hash_value))
        except ClientError:
            return False

        return True


ls = S3Storage({})


@click.group()
def main():
    """Local storage manager"""
    pass


@main.command("store-file")
@click.argument("file-path", type=str)
@click.argument("file-hash", type=str)
def store_file(file_path, file_hash):
    ls.store_file(file_path, file_hash)
