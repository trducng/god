import os
from pathlib import Path
import shutil


def get_symlinks(root):
    """Get non-symlink files in folder `root` (recursively)

    # Args
        root: the path to begin checking for files.

    # Returns
        <[Path, str]>: fn, fh
    """
    non_links = []
    root = Path(root)

    if root.is_symlink():
        return [(root, root.resolve())]

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


def unlock(path):
    """Unlock path from symlinks to files

    # Args
        path <[str]>: the path to unlock, can be file or directory
    """
    symlinks = []
    if isinstance(path, str):
        path = [path]

    for each_path in path:
        # Collect symlinks
        symlinks += get_symlinks(str(each_path))   # symlink, original

    # Copy files
    for symlink, original in symlinks:
        symlink.unlink()
        shutil.copyfile(original, symlink)


if __name__ == '__main__':
    unlock()
