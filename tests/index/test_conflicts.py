import shutil
import unittest
from pathlib import Path

from god.index.base import Index


class ConflictTest(unittest.TestCase):
    def setUp(self):
        self.cache_dir = Path(".cache/tests/index")
        if self.cache_dir.is_dir():
            shutil.rmtree(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self._index_path = self.cache_dir / "conflicts"
        self.items = {
            # untouched
            "n1": {"hash": "h1"},
            # add-add-unresolved
            "n2": {"hash": "h2", "ctheirs": "ct2", "cbase": "cb2"},
            # add-add-resolved whatever solution
            "n3": {
                "hash": "h3",
                "mhash": "mh3",
                "ctheirs": "ct3",
                "cbase": "cb3",
            },
            # add-remove-unresolved
            "n4": {"hash": "h4", "ctheirs": "", "cbase": "cb4"},
            # add-remove-resolved ours
            "n5": {"hash": "h5", "mhash": "mh5", "ctheirs": "", "cbase": "cb5"},
            # add-remove-resolved theirs
            "n6": {"hash": "h6", "ctheirs": "", "cbase": "cb6", "remove": 1},
            # remove-add-unresolved
            "n7": {"ctheirs": "ct7", "cbase": "cb7"},
            # remove-add-resolved ours
            "n8": {"ctheirs": "ct8", "cbase": "cb8", "remove": 1},
            # remove-add-resolved theirs
            "n9": {"ctheirs": "ct9", "cbase": "cb9", "mhash": "mh9"},
        }
        index = Index(self._index_path)
        index.build()
        with Index(self._index_path) as index:
            for name, attrs in self.items.items():
                index.save(name=name, **attrs)

    def test_get_conflict_add_add_all(self):
        """Entries that have `hash` and `ctheirs`"""
        with Index(self._index_path) as index:
            items = index.get_conflict_add_add(case=1)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 2)
        self.assertIn("n2", names)
        self.assertIn("n3", names)

    def test_get_conflict_add_add_unresolved(self):
        """Entries that have `hash` and `ctheirs` while mhash and remove blank"""
        with Index(self._index_path) as index:
            items = index.get_conflict_add_add(case=2)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 1)
        self.assertIn("n2", names)

    def test_get_conflict_add_add_resolved(self):
        """Entries that have `hash`, `mhash` and `ctheirs`"""
        with Index(self._index_path) as index:
            items = index.get_conflict_add_add(case=3)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 1)
        self.assertIn("n3", names)

    def test_get_conflict_add_remove_all(self):
        """Retrieve entries that have `hash` and `ctheirs` == ''"""
        with Index(self._index_path) as index:
            items = index.get_conflict_add_remove(case=1)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 3)
        self.assertCountEqual(["n4", "n5", "n6"], names)

    def test_get_conflict_add_remove_unresolved(self):
        """Retrieve entries that have `hash` and `ctheirs` == '' only"""
        with Index(self._index_path) as index:
            items = index.get_conflict_add_remove(case=2)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 1)
        self.assertCountEqual(["n4"], names)

    def test_get_conflict_add_remove_resolved_ours(self):
        """Retrieve entries that have `hash`, `mhash` and `ctheirs` == ''"""
        with Index(self._index_path) as index:
            items = index.get_conflict_add_remove(case=3)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 1)
        self.assertCountEqual(["n5"], names)

    def test_get_conflict_add_remove_resolved_theirs(self):
        """Retrieve entries that have `hash`, `remove` and `ctheirs` == ''"""
        with Index(self._index_path) as index:
            items = index.get_conflict_add_remove(case=4)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 1)
        self.assertCountEqual(["n6"], names)

    def test_get_conflict_remove_add_all(self):
        """Retrieve entries that have `ctheirs` but not `hash`"""
        with Index(self._index_path) as index:
            items = index.get_conflict_remove_add(case=1)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 3)
        self.assertCountEqual(["n7", "n8", "n9"], names)

    def test_get_conflict_remove_add_unresolved(self):
        """Retrieve entries that have `ctheirs` but not `hash` only"""
        with Index(self._index_path) as index:
            items = index.get_conflict_remove_add(case=2)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 1)
        self.assertCountEqual(["n7"], names)

    def test_get_conflict_remove_add_resolved_ours(self):
        """Retrieve entries that have `ctheirs` and `remove` but not `hash`"""
        with Index(self._index_path) as index:
            items = index.get_conflict_remove_add(case=3)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 1)
        self.assertCountEqual(["n8"], names)

    def test_get_conflict_remove_add_resolved_theirs(self):
        """Retrieve entries that have `ctheirs` and `mhash` but not `hash`"""
        with Index(self._index_path) as index:
            items = index.get_conflict_remove_add(case=4)
            names = [_[0] for _ in items]

        self.assertEqual(len(items), 1)
        self.assertCountEqual(["n9"], names)
