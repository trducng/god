import os
from pathlib import Path
import shutil

from constants import BASE_DIR, GOD_DIR, HASH_DIR


def get_symlinks(root):
    """Get non-symlink files in folder `root` (recursively)

    # Args
        root: the path to begin checking for files.

    # Returns
        <[Paths]>: list of paths to non-symlink files
    """
    non_links = []
    for child in os.scandir(root):
        if child.is_symlink():
            child_path = Path(child.path)
            non_links.append((child_path, child_path.resolve()))
            continue

        if child.is_dir():
            if child.name == '.god':
                continue
            non_links += get_symlinks(child)

    return non_links

def unlock():

    # Collect symlinks
    symlinks = get_symlinks(BASE_DIR)   # symlink, original

    # Copy files
    for symlink, original in symlinks:
        symlink.unlink()
        shutil.copyfile(original, symlink)


if __name__ == '__main__':
    unlock()
