import difflib
import os
import tempfile

from binaryornot.check import is_binary

from god.utils.process import communicate


def _diff_text(path1, path2, fh1, fh2):
    with open(path1, "r") as f_in:
        f1 = f_in.read().splitlines()
    with open(path2, "r") as f_in:
        f2 = f_in.read().splitlines()
    for line in difflib.unified_diff(f1, f2, fromfile=fh1, tofile=fh2, lineterm=""):
        print(line)


def _diff_file(path1, path2, fh1, fh2):
    print(f"Version {fh1}: {os.stat(path1).st_size}")
    print(f"Version {fh2}: {os.stat(path2).st_size}")


def show_diff(add, update, remove):
    """Show to console the diff in files"""
    for fn, _ in add.items():
        print(f"==== Add: {fn}")
    for fn, (fh1, fh2) in update.items():
        print(f"==== Update {fn}")

        fd1, temp_path1 = tempfile.mkstemp()
        fd2, temp_path2 = tempfile.mkstemp()
        communicate(
            command=["god", "storages", "get-objects"], stdin=[[temp_path1, fh1]]
        )
        communicate(
            command=["god", "storages", "get-objects"], stdin=[[temp_path2, fh2]]
        )

        if is_binary(temp_path1):
            _diff_file(temp_path1, temp_path2, fh1, fh2)
        else:
            _diff_text(temp_path1, temp_path2, fh1, fh2)

        os.close(fd1)
        os.close(fd2)
        os.unlink(temp_path1)
        os.unlink(temp_path2)
    for fn, _ in remove.items():
        print(f"==== Add: {fn}")


def diff(add, update, remove):
    """Calculate the diff between files

    Args:
        add: {fn: fh}
        update: {fn: [old-hash, new-hash]}
        remmove: {fn: fh}
    """
    show_diff(add, update, remove)
