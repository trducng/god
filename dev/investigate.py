import hashlib
import os
import random
import time
from pathlib import Path



def create_hash_folder_structure(root, n_files=1e6, start_idx=0):
    """Create random folder and files"""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)

    for idx in range(int(n_files)):
        filepath = hashlib.sha256(f'{idx+start_idx}'.encode()).hexdigest()
        filepath = Path(
            root,
            filepath[:2], filepath[2:4], filepath[4:6], filepath[6:8], filepath[8:10],
            filepath[10:])
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.open('a').close()

def create_symlink(source_root, target_root):
    target_root = Path(target_root)
    target_root.mkdir(parents=True, exist_ok=True)

    for root, _, files in os.walk(source_root):
        for each_file in files:
            sympath = hashlib.sha256(each_file.encode()).hexdigest()
            sympath = Path(
                target_root, sympath[:2], sympath[2:4], sympath[4:6], sympath[6:])
            sympath.parent.mkdir(parents=True, exist_ok=True)
            sympath.symlink_to(os.path.join(root, each_file))


def change_symlink_to_files(root, prob=0.5):
    for base, _, files in os.walk(root):
        for each_file in files:
            if int(each_file[-2:], 16) < 255 * prob:
                filepath = Path(base, each_file)
                filepath.unlink()
                filepath.open('a').close()


def check_symlink(root):
    pass


if __name__ == '__main__':

    # start_time = time.time()
    # create_hash_folder_structure(root, n_files=1e5, start_idx=0)
    # print(f'Create folders and files in {time.time() - start_time} seconds')

    # start_time = time.time()
    # create_symlink('/home/john/datasets/temp/objects', '/home/john/datasets/temp/symlink')
    # print(f'Create symlink in {time.time() - start_time} seconds')

    start_time = time.time()
    change_symlink_to_files('/home/john/datasets/temp/symlink')
    print(f'Change symlink to file in {time.time() - start_time} seconds')

    # start_time = time.time()
    # check_symlink(root)
    # print(f'Check for symlinks and files in {time.time() - start_time} seconds')

