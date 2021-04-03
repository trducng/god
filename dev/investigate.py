import os
import random
import time
from pathlib import Path


def create_random_files(root, n_files=10):
    pass


def create_random_folders(root, n_folders=10):
    pass


def create_random_folders_files(root, n_folders=1000, n_files=1e6):
    pass


def create_symlink(source_root, target_root):
    pass


def change_symlink_to_files(root, prob=0.5):
    pass


def check_symlink(root):
    pass


if __name__ == '__main__':

    start_time = time.time()
    create_random_folders_files(root)
    print(f'Create folders and files in {time.time() - start_time seconds')

    start_time = time.time()
    create_symlink(root, target_root)
    print(f'Create symlink in {time.time() - start_time seconds')

    start_time = time.time()
    change_symlink_to_files(root)
    print(f'Change symlink to file in {time.time() - start_time seconds')

    start_time = time.time()
    check_symlink(root)
    print(f'Check for symlinks and files in {time.time() - start_time seconds')

