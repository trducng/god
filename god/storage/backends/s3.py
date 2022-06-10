import posixpath
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import List

import boto3
from botocore.errorfactory import ClientError
from tqdm import tqdm

from god.storage.backends.base import BaseStorage, RemoteRefsMixin

DEFAULT_DIR_LEVEL = 2


def download_worker(task):
    bucket, key, output = task
    s3r = boto3.resource("s3")
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    s3r.Object(bucket, key).download_file(output)


def upload_worker(task):
    bucket, key, input_ = task
    client = boto3.session.Session().client("s3")
    client.upload_file(input_, bucket, key)


def parse_config(config: str):
    """Parse bucket/prefix s3://bucket/[prefix]"""
    if config.startswith("s3://"):
        config = config[5:]
    else:
        raise ValueError(f'Expect "s3://" but receive {config} instead')
    components = config.split("/")
    bucket = components[0]
    prefix = "/".join(components[1:])
    return bucket, prefix


class S3Storage(BaseStorage, RemoteRefsMixin):
    """Store objects locally"""

    def __init__(self, config: str):
        # TODO: decide the format for storage config
        # TODO: might only allow relative path (to avoid overwrite hacking)
        # self._bucket = config["BUCKET"]
        # @PRIORITY2: remove these default values, raise error if users do not
        # supply these values
        self._bucket, self._prefix = parse_config(config)
        self._dir_levels = DEFAULT_DIR_LEVEL

    def _hash_path(self, hash_value: str, prefix: str = "") -> str:
        """From hash value, get relative hash path"""
        components = [
            hash_value[idx * 2 : (idx + 1) * 2] for idx in range(self._dir_levels)
        ]
        return posixpath.join(
            self._prefix, prefix, *components, hash_value[self._dir_levels * 2 :]
        )

    def _get(self, storage_paths: List[str], paths: List[str]):
        """Get the file and store that file in `paths`

        Args:
            storage_path: the path from storage
            paths: the file path to copy to
        """
        tasks = []
        for storage_path, path in zip(storage_paths, paths):
            tasks.append((self._bucket, storage_path, path))

        with Pool(cpu_count()) as p:
            for _ in p.imap_unordered(download_worker, iterable=tasks):
                continue

    def _store(self, storage_paths: List[str], paths: List[str]):
        """Store a file with a specific hash value

        Args:
            storage_paths: the path from storage
            paths: the file path to send to storage
        """
        tasks = []
        for storage_path, path in zip(storage_paths, paths):
            tasks.append((self._bucket, storage_path, path))

        with Pool(cpu_count()) as p:
            with tqdm(total=len(tasks)) as pbar:
                for _ in p.imap_unordered(upload_worker, iterable=tasks):
                    pbar.update()

    def _delete(self, storage_paths: List[str]):
        """Delete object

        Args:
            storage_paths: the location of object to delete
        """
        # PRIORITY3: make use of bulk delete
        s3r = boto3.resource("s3")
        for storage_path in storage_paths:
            s3r.Object(self._bucket, storage_path).delete()

    def _have(self, storage_paths: List[str]) -> List[bool]:
        """Check whether a file with specific location exists in the storage

        Args:
            storage_paths: the location of the file

        Returns:
            True if the file exists, False otherwise
        """
        result = []
        s3c = boto3.client("s3")
        for storage_path in storage_paths:
            try:
                s3c.head_object(Bucket=self._bucket, Key=storage_path)
                result.append(True)
            except ClientError:
                result.append(False)

        return result

    def _list(self, storage_prefix: str) -> List[str]:
        """Return all hashes and location inside the object storage

        Args:
            storage_prefix: the object prefix (should ends with "/")
        """
        result = []
        s3c = boto3.client("s3")

        paginator = s3c.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self._bucket, Prefix=f"{storage_prefix}")
        for page in pages:
            for each in page["Contents"]:
                result.append(
                    each["Key"].replace(f"{storage_prefix}", "").replace("/", "")
                )
        return result

    def _ref_path(self, ref: str, prefix: str = "") -> str:
        """Construct the path to ref file

        Args:
            ref: the name of the ref
            prefix: the object prefix
        """
        return posixpath.join(self._prefix, prefix, ref)
