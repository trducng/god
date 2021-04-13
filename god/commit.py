"""
Commit the data for hashing
"""
import hashlib
import os
from multiprocessing import Process, Pool
from pathlib import Path
import shutil

from constants import BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR


def get_nonsymlinks(root):
    """Get non-symlink files in folder `root` (recursively)

    # Args
        root: the path to begin checking for files.

    # Returns
        <[Paths]>: list of paths to non-symlink files
    """
    non_links = []
    for child in os.scandir(root):
        if child.is_symlink():
            continue

        if child.is_dir():
            if child.name == '.god':
                continue
            non_links += get_nonsymlinks(child)
        else:
            non_links.append(child.path)

    return non_links


def commit():

    # Collect files
    files = get_nonsymlinks(BASE_DIR)

    # Calculate hash
    hash_table = {}
    for each_file in files:
        with open(each_file, 'rb') as f_in:
            file_hash = hashlib.sha256(f_in.read()).hexdigest()
            hash_path = f'{file_hash[:2]}/{file_hash[2:4]}/{file_hash[4:]}'
            hash_table[each_file] = hash_path

    # Record the add
    add_records = []
    for src, dest in hash_table.items():
        dest = Path(HASH_DIR, dest)
        if dest.is_file():      # NOTE: files can be moved here,
                                # or can be unchanged here after unlock
            continue
        add_records.append((src, dest))

    # Save the records
    out_file = Path(MAIN_DIR, 'temp_record')
    out_records = [f'Add {Path(src).relative_to(BASE_DIR)}' for src, _ in add_records]
    with out_file.open('w') as f_out:
        f_out.write('\n'.join(out_records))
    with out_file.open('rb') as f_in:
        hash_name = hashlib.sha256(f_in.read()).hexdigest()
    shutil.move(out_file, Path(out_file.parent, hash_name))

    # Move objects to hash directory
    for src, dest in add_records:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(src, dest)

    # Create symlink
    for src, dest in add_records:
        sympath = Path(src)
        sympath.symlink_to(dest)


if __name__ == '__main__':
    commit()
