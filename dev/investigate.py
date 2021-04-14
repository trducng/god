import hashlib
import os
import random
import time
from pathlib import Path



def create_hash_folder_structure(root, n_files=1e6, start_idx=0):
    """Create random folder and files

    The files and folder structure are created randomly based on hash.

    # Args
        root: the parent directory
        n_files: the number of files to create
        start_idx: the start of index counter
    """
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
    """Create symlink recursively of files from `source_root` to `target_root`

    The folder structure of `target_root` will be random based on hash

    # Args:
        source_root: the parent directory of original files
        target_root: the symlink will be created inside this directory
    """
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
    """Randomly change the symlinks in `root` from to files

    # Args
        root: the folder to begin recursively change file
        prob: probability of a symlink be converted to a file
    """
    for base, _, files in os.walk(root):
        for each_file in files:
            if int(each_file[-2:], 16) < 255 * prob:
                filepath = Path(base, each_file)
                filepath.unlink()
                filepath.open('a').close()


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
            non_links += get_nonsymlinks(child)
        else:
            non_links.append(child.path)

    return non_links


from multiprocessing import Process, Pool
def get_nonsymlinks_worker(root, output=None):
    """Get non-symlink files in folder `root` (recursively)

    # Args
        root: the path to begin checking for files.

    # Returns
        <[Paths]>: list of paths to non-symlink files
    """
    non_links = [] for child in os.scandir(root):
        if child.is_symlink():
            continue

        if child.is_dir():
            non_links += get_nonsymlinks(child)
        else:
            non_links.append(child.path)

    if output is not None:
        with open(output, 'w') as f_out:
            f_out.write('\n'.join(non_links))
    else:
        return non_links


def get_nonsymlinks_mp(root, cache):
    non_links = []
    counter = 1
    process_list = []

    for child in os.scandir(root):
        if child.is_symlink():
            continue

        if child.is_dir():
            process_list = [each for each in process_list if each.is_alive()]
            if len(process_list) < 8:
                print(f'=> Run {child.path} in process')
                p = Process(
                    target=get_nonsymlinks_worker,
                    args=(child, Path(cache, f'{counter}.txt')))
                p.start()
                process_list.append(p)
            else:
                print(f'    => Run {child.path} in main')
                get_nonsymlinks_worker(child, Path(cache, f'{counter}.txt'))

            counter += 1
        else:
            non_links.append(child.path)

    with open(Path(cache, '0.txt'), 'w') as f_out:
        f_out.write('\n'.join(non_links))


if __name__ == '__main__':

    root = '/home/john/temp/benchmark-god/hashes'
    symlink_folder = '/home/john/temp/benchmark-god/symlink'
    cache_folder = '/home/john/temp/benchmark-god/cache'

    # start_time = time.time()
    # create_hash_folder_structure(root, n_files=1e5, start_idx=0)
    # print(f'Create folders and files in {time.time() - start_time} seconds')

    # start_time = time.time()
    # create_symlink(root, symlink_folder)
    # print(f'Create symlink in {time.time() - start_time} seconds')

    # start_time = time.time()
    # change_symlink_to_files(symlink_folder)
    # print(f'Change symlink to file in {time.time() - start_time} seconds')

    start_time = time.time()
    result = get_nonsymlinks_mp(symlink_folder, cache_folder)
    print(f'Check for symlinks and files in {time.time() - start_time} seconds')

