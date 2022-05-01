import posixpath
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import List

import boto3
from botocore.errorfactory import ClientError
from tqdm import tqdm

import god.storage.constants as c
from god.storage.backends.base import BaseStorage

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


class S3Storage(BaseStorage):
    """Store objects locally"""

    def __init__(self, config: str):
        # TODO: decide the format for storage config
        # TODO: might only allow relative path (to avoid overwrite hacking)
        # self._bucket = config["BUCKET"]
        # @PRIORITY2: remove these default values, raise error if users do not
        # supply these values
        self._bucket, self._prefix = parse_config(config)
        self._object_path = posixpath.join(self._prefix, c.DIR_OBJECTS)
        self._dir_path = posixpath.join(self._prefix, c.DIR_DIRS)
        self._commit_path = posixpath.join(self._prefix, c.DIR_COMMITS)
        self._dir_levels = DEFAULT_DIR_LEVEL

    def _get_hash_path(self, hash_value: str) -> str:
        """From hash value, get relative hash path"""
        components = [
            hash_value[idx * 2 : (idx + 1) * 2] for idx in range(self._dir_levels)
        ]
        return posixpath.join(*components, hash_value[self._dir_levels * 2 :])

    def _get(self, path: str, hash_values: List[str], file_paths: List[str]):
        """Get the file and store that file in file_path

        Args:
            path: whether object, dir or commit path
            hash_value: the object hash value
            file_path: the file path to copy to
        """
        tasks = []
        for each_hash, each_path in zip(hash_values, file_paths):
            tasks.append(
                (
                    self._bucket,
                    posixpath.join(path, self._get_hash_path(each_hash)),
                    each_path,
                )
            )
        with Pool(cpu_count()) as p:
            for _ in p.imap_unordered(download_worker, iterable=tasks):
                continue

    def _store(self, path: str, file_paths: List[str], hash_values: List[str]):
        """Store a file with a specific hash value

        Args:
            file_path: the file path
            hash_value: the hash value of the file
        """
        tasks = []
        for each_hash, each_file in zip(hash_values, file_paths):
            tasks.append(
                (
                    self._bucket,
                    posixpath.join(path, self._get_hash_path(each_hash)),
                    each_file,
                )
            )
        with Pool(cpu_count()) as p:
            with tqdm(total=len(tasks)) as pbar:
                for _ in p.imap_unordered(upload_worker, iterable=tasks):
                    pbar.update()

    def _delete(self, path: str, hash_values: List[str]):
        """Delete object that has specific hash value

        Args:
            hash_value: the hash value of the object
        """
        # PRIORITY3: make use of bulk delete
        s3r = boto3.resource("s3")
        for each_hash in hash_values:
            s3r.Object(
                self._bucket, posixpath.join(path, self._get_hash_path(each_hash))
            ).delete()

    def _have(self, path: str, hash_values: List[str]) -> List[bool]:
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
                    Bucket=self._bucket,
                    Key=posixpath.join(path, self._get_hash_path(hash_value)),
                )
                result.append(True)
            except ClientError:
                result.append(False)

        return result

    def _hash(self, path: str) -> List[str]:
        """Return all hashes inside the object storage"""
        return []

    ### objects
    def get_objects(self, hash_values: List[str], paths: List[str]):
        return self._get(self._object_path, hash_values, paths)

    def store_objects(self, paths: List[str], hash_values: List[str]):
        return self._store(self._object_path, paths, hash_values)

    def delete_objects(self, hash_values: List[str]):
        return self._delete(self._object_path, hash_values)

    def have_objects(self, hash_values: List[str]) -> List[bool]:
        return self._have(self._object_path, hash_values)

    def list_objects(self) -> List[str]:
        return self._hash(self._object_path)

    ### dirs
    def get_dirs(self, hash_values: List[str], paths: List[str]):
        return self._get(self._dir_path, hash_values, paths)

    def store_dirs(self, paths: List[str], hash_values: List[str]):
        return self._store(self._dir_path, paths, hash_values)

    def delete_dirs(self, hash_values: List[str]):
        return self._delete(self._dir_path, hash_values)

    def have_dirs(self, hash_values: List[str]) -> List[bool]:
        return self._have(self._dir_path, hash_values)

    def list_dirs(self) -> List[str]:
        return self._hash(self._dir_path)

    ### commits
    def get_commits(self, hash_values: List[str], paths: List[str]):
        return self._get(self._commit_path, hash_values, paths)

    def store_commits(self, paths: List[str], hash_values: List[str]):
        return self._store(self._commit_path, paths, hash_values)

    def delete_commits(self, hash_values: List[str]):
        return self._delete(self._commit_path, hash_values)

    def have_commits(self, hash_values: List[str]) -> List[bool]:
        return self._have(self._commit_path, hash_values)

    def list_commits(self) -> List[str]:
        return self._hash(self._commit_path)
