"""Provide CLI-compatible adapter"""
import fire

from god.init import init
from god.commit import commit
from god.base import settings


class CLI:

    def init(self, path='.'):
        """Initiate the repo"""
        init(path)

    def commit(self, path, debug=False):
        """Run the commit function"""
        commit(path)

    def check(self, **kwargs):
        from god.base import Settings
        from god.commit import play_with_setting
        # settings1 = Settings()
        # settings1.set_values_from_yaml('/home/john/temp/god/config4.yml')
        # settings2 = Settings()
        # settings2.set_values_from_yaml('/home/john/temp/god/config3.yml')
        # print(settings1 + settings2)
        settings.set_global_settings(debug=True)
        play_with_setting()

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
