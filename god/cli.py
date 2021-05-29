"""Provide CLI-compatible adapter"""
from pathlib import Path

import fire

from god.init import init
from god.commit import commit
from god.base import settings
from god.history import get_history
from god.unlock import unlock


class CLI:

    def init(self, path='.', **kwargs):
        """Initiate the repo"""
        init(path)

    def commit(self, path, **kwargs):
        """Run the commit function"""
        settings.set_global_settings()
        commit(path)

    def logs(self, **kwargs):
        settings.set_global_settings(**kwargs)
        get_history()

    def index(self, **kwargs):
        from god.orge import construct_sql_logs
        from god.logs import get_transform_operations
        settings.set_global_settings(**kwargs)
        history = get_history()
        # file_add, file_remove = get_transform_operations(history[1])
        # result = construct_sql_logs(file_add, file_remove, settings.INDEX, name='index', state=history[1])
        file_add, file_remove = get_transform_operations(history[1], history[0])
        result = construct_sql_logs(file_add, file_remove, settings.INDEX, name='index', state=history[0])
        import pdb; pdb.set_trace()

    def search(self, index=None, columns=None, **kwargs):
        """
        Example usage:
            god search index --col1 "value1||value2" --col2 "valuea"
        """
        import time
        from god.search import search
        settings.set_global_settings()
        if columns is not None:
            columns = columns.split(',')

        result = search(settings.INDEX, index, columns, **kwargs)
        with Path(settings.DIR_CWD, 'god.godsnap').open('w') as f_out:
            query = ','.join(f'{key}:{value}' for key, value in kwargs.items())
            f_out.write(f'# Time: {time.time()}\n')
            f_out.write(f'# Index: {index}\n')
            f_out.write(f'# Query: {query}\n')
            f_out.write('='*88)
            for each in result:
                f_out.write('\n')
                f_out.write(','.join(each))

        import pdb; pdb.set_trace()

    def update(self, index, operation, target, **kwargs):
        """Update feature attributes

        god update index add folder --features risk

        @TODO: problem because of * expansion in zsh shell
        """
        from god.update import update
        settings.set_global_settings()
        result = update(str(target), operation, settings.INDEX, index, **kwargs)
        import pdb; pdb.set_trace()

    def unlock(self, *args, **kwargs):
        """Unlock file from symlink to normal"""
        cwd = Path.cwd()
        path = [str(cwd / each) for each in args]
        unlock(path)

    # def snap(self, operation, file_path, name):
    def snap(self, operation):
        from god.snap import add, list_snap
        settings.set_global_settings()
        # file_path = Path(file_path).resolve()
        if operation == 'add':
            print(add(file_path, name))
        elif operation == 'list':
            print(list_snap())

    def check(self, **kwargs):
        from god.base import Settings
        from god.commit import play_with_setting
        # settings1 = Settings()
        # settings1.set_values_from_yaml('/home/john/temp/god/config4.yml')
        # settings2 = Settings()
        # settings2.set_values_from_yaml('/home/john/temp/god/config3.yml')
        # print(settings1 + settings2)
        settings.set_global_settings(debug=True)
        print(settings)
        import pdb; pdb.set_trace()

    def debug(self, command, *args, **kwargs):
        """Run in debug mode"""

        from pdb import Pdb
        import sys
        import traceback

        pdb = Pdb()

        try:
            self.__getattribute__(command)(*args, **kwargs)
        except:
            traceback.print_exc()
            print("Uncaught exception. Entering post mortem debugging")
            t = sys.exc_info()[2]
            pdb.interaction(None, t)


if __name__ == '__main__':
    fire.Fire(CLI)
