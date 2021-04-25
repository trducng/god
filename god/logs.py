import hashlib
from pathlib import Path

from constants import BASE_DIR, LOG_DIR


def get_log_records(files, hashes):
    """Construct log records"""
    out_records = [
        f'+{Path(each_file).relative_to(BASE_DIR)} {each_hash}'
        for each_file, each_hash in zip(files, hashes)
    ]

    return out_records


def save_log(add_records, remove_records):
    """Construct the logs based on add_records and remove_records

    The log has format:
        + file_path1 file_hash1
        - file_path2 file_hash1

    # Args
        add_records <[(str, str)]>: file path and hash
        remove_records <[(str, str)]>: file path and hash

    # Returns
        <str>: hash of the log files
    """
    add_records = [
            f'+ {file_path} {file_hash}' for (file_path, file_hash) in add_records]
    remove_records = [
            f'- {file_path} {file_hash}' for (file_path, file_hash) in remove_records]
    records = '\n'.join(add_records + remove_records)
    hash_name = hashlib.sha256(records.encode()).hexdigest()

    with Path(LOG_DIR, hash_name).open('w') as f_out:
        f_out.write(records)

    return hash_name
