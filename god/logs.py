import hashlib
from pathlib import Path
import shutil

from constants import BASE_DIR, LOG_DIR


def get_log_records(files, hashes):
    """Construct log records"""
    out_records = [
        f'+{Path(each_file).relative_to(BASE_DIR)} {each_hash}'
        for each_file, each_hash in zip(files, hashes)
    ]

    return out_records

def save_log(records):
    out_file = Path(LOG_DIR, 'temp_record')
    with out_file.open('r') as f_out:
        f_out.write('\n'.join(records))
    with out_file.open('rb') as f_in:
        hash_name = hashlib.sha256(f_in.read()).hexdigest()
    shutil.move(out_file, Path(out_file.parent, hash_name))
