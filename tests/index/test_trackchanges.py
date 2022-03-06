"""Test ability to track changes from index"""
import unittest
from pathlib import Path

from god.index.trackchanges import (
    track_files,
    track_staging_changes,
    track_working_changes,
)


class TrackChangesTest(unittest.TestCase):
    def setUp(self):
        """Setup a fake scenario"""
        self.working_dir = str(Path("tests", "assets", "index").resolve())
        self.index_path = str(Path(self.working_dir, ".god", "files"))

    def test_track_working_changes(self):
        add, update, remove, reset_tst, unset_mhash = track_working_changes(
            fds=[self.working_dir],
            index_path=self.index_path,
            base_dir=self.working_dir,
        )

    def test_track_staging_changes(self):
        add, update, remove = track_staging_changes(
            fds=[self.working_dir],
            index_path=self.index_path,
            base_dir=self.working_dir,
        )

    def test_track_files(self):
        s_add, s_update, s_remove, add, update, remove, tst, mhash = track_files(
            fds=[self.working_dir],
            index_path=self.index_path,
            base_dir=self.working_dir,
        )
