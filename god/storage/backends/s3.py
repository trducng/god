import posixpath
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from multiprocessing import Pool, Process, Queue, cpu_count
from pathlib import Path
from typing import Callable, List, Tuple, Union

import boto3
from botocore.errorfactory import ClientError
from tqdm import tqdm

from god.storage.backends.base import BaseStorage, RemoteRefsMixin

DEFAULT_DIR_LEVEL = 2


def download_process(
    process_idx: int,
    in_queue: Queue,
    out_queue: Queue,
    progress_queue: Union[Queue, None],
):
    """S3 download process, communicate through queue

    If `progress_queue` is None, don't report progress.
    """

    class _DownloadProgressCallback:
        def __init__(self, progress_queue, process_idx, total_files, total_bytes):
            self.progress_queue = progress_queue
            self.process_idx = process_idx
            self.total_files = total_files
            self.total_bytes = total_bytes
            self._lock = threading.Lock()

        def __call__(self, bytes_amount):
            with self._lock:
                self.total_bytes += bytes_amount
                self.progress_queue.put(
                    {
                        "process_idx": self.process_idx,
                        "total_files": self.total_files,
                        "total_bytes": self.total_bytes,
                    }
                )

    client = boto3.client("s3")
    total_files, total_bytes = 0, 0
    while True:
        message = in_queue.get()
        if message is None:
            break

        progress_callback = (
            _DownloadProgressCallback(
                progress_queue, process_idx, total_files, total_bytes
            )
            if progress_queue
            else None
        )
        bucket, key, output_path = message
        client.download_file(bucket, key, output_path, Callback=progress_callback)
        out_queue.put(process_idx)

        if progress_callback:
            total_files += 1
            total_bytes = progress_callback.total_bytes
            progress_queue.put(  # type: ignore (progress_queue cannot be None)
                {
                    "process_idx": process_idx,
                    "total_files": total_files,
                    "total_bytes": total_bytes,
                }
            )


def _download_progress_process(progress_queue: Queue, callback: Callable):
    """Consolidate download progress of different processes into 1 report

    Args:
        progress_queue: the queue contain progress, contributed by all download workers
        callback: callback function that will receive total downloaded files and total
            downloaded bytes
    """
    progress = {}
    while True:
        current_progress = progress_queue.get()
        if current_progress["process_idx"] == -1:
            callback(None, None)
            break
        progress[current_progress["process_idx"]] = (
            current_progress["total_files"],
            current_progress["total_bytes"],
        )
        total_files, total_bytes = 0, 0
        for each_progress in progress.values():
            total_files += each_progress[0]
            total_bytes += each_progress[1]
        callback(total_files, total_bytes)


def upload_worker(task):
    bucket, key, input_ = task
    client = boto3.session.Session().client("s3")
    client.upload_file(input_, bucket, key)


def _object_exists_worker(client, bucket: str, prefix: str) -> bool:
    """Check if an object exists in S3"""
    try:
        client.head_object(Bucket=bucket, Key=prefix)
    except ClientError:
        return False

    return True


def parse_config(config: str) -> Tuple[str, str]:
    """Parse bucket/prefix s3://bucket/[prefix]

    Args:
        config: the configuration string

    Returns:
        - the S3 bucket
        - the path prefix
    """
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
        # TODO: might only allow relative path (to avoid overwrite hacking)
        # self._bucket = config["BUCKET"]
        # @PRIORITY2: remove these default values, raise error if users do not
        # supply these values
        self._bucket, self._prefix = parse_config(config)
        self._dir_levels = DEFAULT_DIR_LEVEL

        s3c = boto3.client("s3")
        try:
            s3c.head_bucket(Bucket=self._bucket)
        except ClientError:
            raise Exception(
                f"Bucket {self._bucket} does not exist or you don't have credentials"
            )

    def _hash_path(self, hash_value: str, prefix: str = "") -> str:
        """From hash value, get relative hash path"""
        components = [
            hash_value[idx * 2 : (idx + 1) * 2] for idx in range(self._dir_levels)
        ]
        return posixpath.join(
            self._prefix, prefix, *components, hash_value[self._dir_levels * 2 :]
        )

    def _get(
        self,
        storage_paths: List[str],
        paths: List[str],
        progress_callback: Union[Callable, None] = None,
        n_processes: Union[int, None] = None,
    ):
        """Get the file and store that file in `paths`

        Args:
            storage_path: the path from storage
            paths: the file path to copy to
            progress_callback: callback on how to report progress
            n_processes: the number of processes to spin up, default equal to the
                number of CPU cores
        """
        if len(storage_paths) != len(paths):
            raise AttributeError(f"Inconsistent {len(storage_paths)} , {len(paths)}")

        if n_processes is None:
            n_processes = min(cpu_count(), len(paths))

        processes = {}
        in_queues = {}
        out_queue = Queue()
        progress_queue = Queue() if progress_callback is not None else None
        progress_process = None
        current_message_idx = 0

        for process_idx in range(n_processes):
            in_queue = Queue()
            in_queue.put(
                (
                    self._bucket,
                    storage_paths[current_message_idx],
                    paths[current_message_idx],
                )
            )
            in_queues[process_idx] = in_queue
            current_message_idx += 1

            p = Process(
                target=download_process,
                args=(process_idx, in_queue, out_queue, progress_queue),
            )
            processes[process_idx] = p
            p.start()

        if progress_callback:
            progress_process = Process(
                target=_download_progress_process,
                args=(progress_queue, progress_callback),
            )
            progress_process.start()

        n_finished = 0
        while n_finished < n_processes:
            process_idx = out_queue.get()
            if current_message_idx >= len(storage_paths):
                n_finished += 1
                in_queues[process_idx].put(None)
                continue

            in_queues[process_idx].put(
                (
                    self._bucket,
                    storage_paths[current_message_idx],
                    paths[current_message_idx],
                )
            )
            current_message_idx += 1

        if progress_process and progress_queue:
            progress_queue.put({"process_idx": -1})
            progress_process.join()

    def _store(self, storage_paths: List[str], paths: List[str]):
        """Store files with a specific hash values

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
        # PRIORITY3: make use of bulk delete `client.delete_objects`
        s3r = boto3.resource("s3")
        for storage_path in storage_paths:
            s3r.Object(self._bucket, storage_path).delete()

    def _have(self, storage_paths: List[str]) -> List[bool]:
        """Check whether a file with specific location exists in the storage

        Since len(storage_paths) can easily reach 10s of millions

        Args:
            storage_paths: the location of the file

        Returns:
            True if the file exists, False otherwise
        """
        s3c = boto3.client("s3")
        if not s3c.list_objects_v2(Bucket=self._bucket, Prefix=self._prefix, MaxKeys=1)[
            "KeyCount"
        ]:
            return [False] * len(storage_paths)

        result = []
        if len(storage_paths) < 10:
            for storage_path in storage_paths:
                try:
                    s3c.head_object(Bucket=self._bucket, Key=storage_path)
                    result.append(True)
                except ClientError:
                    result.append(False)
        else:
            # good when len(storage_paths) < 100000 (3 mins)
            # but if the dataset is millions file, then multithreading is still slow
            fn = partial(_object_exists_worker, s3c, self._bucket)
            n_workers = int(min(128, len(storage_paths) / 2))
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                result = list(executor.map(fn, storage_paths))

        return result

    def _list(self, storage_prefix: str) -> List[str]:
        """Return all hashes and location inside the object storage

        Args:
            storage_prefix: the object prefix (should ends with "/")
        """
        result = []
        s3c = boto3.client("s3")

        # PRIORITY3: perform multi-threading
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


if __name__ == "__main__":
    import json
    import time

    local_storage = S3Storage(config="s3://god-test-storage")
    with open("/data/datasets/god-test/s3objects.json", "r") as fi:
        hashes = json.load(fi)[:1000]
    paths = [str(Path("/data/datasets/god-test/temp3", each)) for each in hashes]
    start = time.time()
    local_storage.get_objects(hash_values=hashes, paths=paths)
    duration = time.time() - start
    print(f"Take {duration} seconds")
