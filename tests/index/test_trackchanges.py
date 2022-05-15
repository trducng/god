"""Test ability to track changes from index"""
import hashlib
import random
import shutil
import time
import unittest
from pathlib import Path

from god.index.base import Index
from god.index.trackchanges import (
    track_files,
    track_staging_changes,
    track_working_changes,
)

WORKING_DIR = str(Path(".cache", "index").resolve())
INDEX_PATH = str(Path(WORKING_DIR, ".god", "files"))


def construct_test_folder(output_folder):
    """Construct the test folder for testing"""
    # create
    output_folder = Path(output_folder)
    god_folder = Path(output_folder, ".god")
    god_folder.mkdir(parents=True, exist_ok=True)
    index_file = str(god_folder / "files")
    index = Index(index_path=index_file)
    index.build()

    files = [
        "file1",
        "file2",
        "folder1/file1",
        "folder1/file2",
        "folder2/file1",
        "folder2/file2",
        "folder3/file1",
        "folder3/file2",
        "folder3/foldera/file1",
        "folder3/folderb/file1",
    ]

    # commit stage
    original_files = {fn: f"{fn} Content" for fn in files}
    add = []
    for fn, content in original_files.items():
        time.sleep(2 * random.random())
        fp = Path(output_folder, fn)
        fp.parent.mkdir(exist_ok=True)
        with fp.open("w") as fo:
            fo.write(content)
        add.append(
            [fn, hashlib.sha256(content.encode()).hexdigest(), fp.stat().st_mtime]
        )

    with Index(index_path=index_file) as index:
        index.add(items=add, staged=False)

    # staging stage
    staged_add = {
        "folder4/file1": "folder4/file1 Content",
        "folder4/file2": "folder4/file2 Content",
    }
    stage = []
    for fn, content in staged_add.items():
        time.sleep(2 * random.random())
        fp = Path(output_folder, fn)
        fp.parent.mkdir(exist_ok=True)
        with fp.open("w") as fo:
            fo.write(content)
        stage.append(
            [fn, hashlib.sha256(content.encode()).hexdigest(), fp.stat().st_mtime]
        )

    with Index(index_path=index_file) as index:
        index.add(items=stage, staged=True)

    staged_update = {
        "file1": "file1 Change1",
        "folder2/file1": "folder2/file1 Change1",
        "folder2/file2": "folder2/file2 Change1",
    }
    stage = []
    for fn, content in staged_update.items():
        time.sleep(2 * random.random())
        fp = Path(output_folder, fn)
        with fp.open("w") as fo:
            fo.write(content)
        stage.append(
            [fn, hashlib.sha256(content.encode()).hexdigest(), fp.stat().st_mtime]
        )

    with Index(index_path=index_file) as index:
        index.update(items=stage)

    staged_remove = ["folder3/file1"]
    with Index(index_path=index_file) as index:
        index.delete(items=staged_remove, staged=True)

    # working stage
    working_files = {
        "folder2/file2": "folder2/file2 Change2",
        "folder4/file1": "folder4/file1 Change1",
        "folder4/file3": "folder4/file3 Content",
    }
    for fn, content in working_files.items():
        time.sleep(2 * random.random())
        fp = Path(output_folder, fn)
        fp.parent.mkdir(exist_ok=True)
        with fp.open("w") as fo:
            fo.write(content)


class TrackChangesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Setup a fake scenario"""
        if Path(WORKING_DIR).is_dir():
            shutil.rmtree(WORKING_DIR)
        construct_test_folder(WORKING_DIR)

    @classmethod
    def tearDownClass(cls):
        if Path(WORKING_DIR).is_dir():
            shutil.rmtree(WORKING_DIR)

    def test_track_working_changes(self):
        add, update, remove, reset_tst, unset_mhash = track_working_changes(
            fds=[WORKING_DIR],
            index_path=INDEX_PATH,
            base_dir=WORKING_DIR,
        )

        # check add
        self.assertEqual(len(add), 2)
        self.assertListEqual(
            list(sorted(_[0] for _ in add)),
            ["folder3/file1", "folder4/file3"],
        )

        # check update
        self.assertEqual(len(update), 2)
        self.assertListEqual(
            list(sorted(_[0] for _ in update)),
            ["folder2/file2", "folder4/file1"],
        )

    def test_track_staging_changes(self):
        add, update, remove = track_staging_changes(
            fds=[WORKING_DIR],
            index_path=INDEX_PATH,
            base_dir=WORKING_DIR,
        )

        # check add
        self.assertEqual(len(add), 2)
        self.assertListEqual(
            list(sorted(add)),
            ["folder4/file1", "folder4/file2"],
        )

        # check update
        self.assertEqual(len(update), 3)
        self.assertListEqual(
            list(sorted(update)),
            ["file1", "folder2/file1", "folder2/file2"],
        )

        # check remove
        self.assertEqual(len(remove), 1)
        self.assertListEqual(
            list(sorted(remove)),
            ["folder3/file1"],
        )

    def test_track_files(self):
        s_add, s_update, s_remove, add, update, remove, tst, mhash = track_files(
            fds=[WORKING_DIR],
            index_path=INDEX_PATH,
            base_dir=WORKING_DIR,
        )

        # check s_add
        self.assertEqual(len(s_add), 2)
        self.assertListEqual(
            list(sorted(s_add)),
            ["folder4/file1", "folder4/file2"],
        )

        # check s_update
        self.assertEqual(len(s_update), 3)
        self.assertListEqual(
            list(sorted(s_update)),
            ["file1", "folder2/file1", "folder2/file2"],
        )

        # check s_remove
        self.assertEqual(len(s_remove), 1)
        self.assertListEqual(
            list(sorted(s_remove)),
            ["folder3/file1"],
        )

        # check add
        self.assertEqual(len(add), 2)
        self.assertListEqual(
            list(sorted(_[0] for _ in add)),
            ["folder3/file1", "folder4/file3"],
        )

        # check update
        self.assertEqual(len(update), 2)
        self.assertListEqual(
            list(sorted(_[0] for _ in update)),
            ["folder2/file2", "folder4/file1"],
        )
