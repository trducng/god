"""Provide CLI-compatible adapter"""
from pathlib import Path

import fire

from god.base import settings

# from god.history import get_history
# from god.unlock import unlock
from god.porcelain import (
    add_cmd,
    checkout_cmd,
    commit_cmd,
    config_cmd,
    init_cmd,
    log_cmd,
    merge_cmd,
    record_add_cmd,
    reset_cmd,
    restore_staged_cmd,
    restore_working_cmd,
    status_cmd,
)

# from god.commit import commit


class SnapCLI:
    """Snapshot functionality"""

    def add(self, file_path, name):
        from god.snap import add

        settings.set_global_settings()
        file_path = Path(file_path).resolve()
        print(add(file_path, name))

    def ls(self):
        from god.snap import ls

        settings.set_global_settings()
        print(ls())

    def compare(self, name1, name2):
        from god.snap import compare

        settings.set_global_settings()
        add, remove, update = compare(name1, name2)

    def refresh(self, name1):
        pass


class RecordCLI:
    """Record CLI"""

    def add(self, name, **kwargs):
        """Construct the records logs from commit"""
        settings.set_global_settings(**kwargs)
        record_add_cmd(name)

    def execute(self, **kwargs):
        """Execute the records logs into database, and hold the record logs"""
        pass


class CLI:
    """Command line interface for `god`"""

    def __init__(self):
        self.snap = SnapCLI()
        self.record = RecordCLI()

    def init(self, path=".", **kwargs):
        """Initiate the repo"""
        init_cmd(path)

    def config(self, op, **kwargs):
        """View, update the config

        # Args
            op <str>: can be 'list', 'list-local', 'add'
            **kwargs <{}>: config value to be updated
        """
        settings.set_global_settings()
        result = config_cmd(op, **kwargs)
        if op != "add":
            print(result)

    def status(self, *paths, **kwargs):
        """View repo status"""
        settings.set_global_settings()
        if not paths:
            paths = ["."]
        status_cmd(paths)

    def add(self, *paths, **kwargs):
        """Add files and directories to staging area

        # Args
            *paths <[str]>: list of paths
        """
        settings.set_global_settings()
        add_cmd(paths)

    def commit(self, message, **kwargs):
        """Run the commit function"""
        settings.set_global_settings()
        commit_cmd(message)

    def log(self, **kwargs):
        """Print out history of the god repository"""
        settings.set_global_settings(**kwargs)
        log_cmd()

    def restore(self, *paths, **kwargs):
        """Restore files state"""
        settings.set_global_settings(**kwargs)
        staged = kwargs.pop("staged", False)
        if staged:
            restore_staged_cmd(paths)
        else:
            restore_working_cmd(paths)

    def checkout(self, branch, **kwargs):
        """Checkout

        # Args:
            branch <str>: name of the branch
            new <bool>: whether to create new branch
        """
        settings.set_global_settings()
        new = kwargs.pop("new", False)
        checkout_cmd(branch, new)

    def reset(self, head_past, *arg, **kwargs):
        """Reset the repository to `commit_id`

        # Args:
            head_past <str>: the head past, of format HEAD^x, where x is an integer
            hard <bool>: if true, complete convert to commit_id
        """
        settings.set_global_settings()
        hard = kwargs.pop("hard", False)
        reset_cmd(head_past, hard)

    def merge(self, branch, **kwargs):
        """Merge to target branch `branch`

        # Args:
            branch <str>: name of the branch
        """
        settings.set_global_settings()
        merge_cmd(branch)

    def search(self, index=None, columns=None, **kwargs):
        """
        Example usage:
            god search index --col1 "value1||value2" --col2 "valuea"
        """
        import time

        from god.search import search

        settings.set_global_settings()
        if columns is not None:
            columns = columns.split(",")

        result = search(settings.INDEX, index, columns, **kwargs)
        with Path(settings.DIR_CWD, "god.godsnap").open("w") as f_out:
            query = ",".join(f"{key}:{value}" for key, value in kwargs.items())
            f_out.write(f"# Time: {time.time()}\n")
            f_out.write(f"# Index: {index}\n")
            f_out.write(f"# Query: {query}\n")
            f_out.write("=" * 88)
            for each in result:
                f_out.write("\n")
                f_out.write(",".join(each))

    def update(self, index, operation, target, **kwargs):
        """Update feature attributes

        god update index add folder --features risk

        @TODO: problem because of * expansion in zsh shell
        """
        from god.update import update

        settings.set_global_settings()
        update(str(target), operation, settings.INDEX, index, **kwargs)

    def check(self, **kwargs):
        from pprint import pprint

        pprint(kwargs)

    def debug(self, command, *args, **kwargs):
        """Run in debug mode"""
        import sys
        import traceback
        from pdb import Pdb

        pdb = Pdb()

        try:
            self.__getattribute__(command)(*args, **kwargs)
        except Exception:
            traceback.print_exc()
            print("Uncaught exception. Entering post mortem debugging")
            t = sys.exc_info()[2]
            pdb.interaction(None, t)


if __name__ == "__main__":
    fire.Fire(CLI)
