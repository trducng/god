"""Test merge"""
import unittest

from god.merge import check_merge_plugins


def _fake_commit(**kwargs):
    """Create fake commit"""
    return {"tracks": kwargs}


class CheckMergePluginsTest(unittest.TestCase):
    """Scenarios for check_merge_plugins"""

    def test_update_a_plugin_valid(self):
        """If a plugin is updated in 2 branches, it's still valid"""
        commitp = _fake_commit(files="parent_commit-hash")
        commit1 = _fake_commit(files="commit_obj1-hash")
        commit2 = _fake_commit(files="commit_obj2-hash")

        valid, invalid = check_merge_plugins(commit1, commit2, commitp)
        self.assertEqual(valid, ["files"])
        self.assertEqual(len(invalid), 0)

    def test_both_add_valid(self):
        """Valid if each branch has a new plugin"""
        commitp = _fake_commit(files="parent_commit-hash")
        commit1 = _fake_commit(files="commit_obj1-hash", plug1="plug1")
        commit2 = _fake_commit(files="commit_obj2-hash", plug2="plug2")

        valid, invalid = check_merge_plugins(commit1, commit2, commitp)
        self.assertCountEqual(valid, ["files", "plug1", "plug2"])
        self.assertEqual(len(invalid), 0)

    def test_add_remove_invalid(self):
        """Inconsistent if a plugin is updated in a branch and removed in a branch"""
        commitp = _fake_commit(files="parent-hash")
        commit1 = _fake_commit(files="commit1-hash")
        commit2 = _fake_commit(config="files-not-exist")

        valid, invalid = check_merge_plugins(commit1, commit2, commitp)
        self.assertEqual(invalid, ["files"])
        self.assertEqual(valid, ["config"])
