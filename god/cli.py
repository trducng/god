"""Provide CLI-compatible adapter"""
import fire

from god.init import init
from god.commit import commit


class CLI:

    def init(self, path='.'):
        """Initiate the repo"""
        init(path)

    def commit(self, path):
        commit(path)


if __name__ == '__main__':
    fire.Fire(CLI)
