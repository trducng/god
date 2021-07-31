"""Test tree comparison"""
import json
import shutil
import unittest
from pathlib import Path

from god.records.compare import (
    compare_leaves,
    compare_tree,
    get_leftmost_leaf,
    get_next_leaf,
    get_next_sibling,
    get_root,
)
from god.records.storage import (
    construct_node,
    get_keys_indices,
    get_keys_values,
    get_leaf_nodes,
    get_matching_child,
    get_paths_to_records,
    get_records,
    prolly_create,
    prolly_delete,
    prolly_edit,
    prolly_insert,
    prolly_locate,
    prolly_update,
)


class CompareNavigateTest(unittest.TestCase):
    def setUp(self):
        self.example_json = Path("./tests/assets/storage_source.json")
        self.cache_dir = Path(".cache/tests/storage/compare")
        self.internal_nodes = Path(self.cache_dir, "nodes")
        self.leaf_nodes = Path(self.cache_dir, "leaves")

        # clean up and create the cache
        if self.cache_dir.is_dir():
            shutil.rmtree(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # create sub-dirs
        self.internal_nodes.mkdir()
        self.leaf_nodes.mkdir()

        # construct tree
        with self.example_json.open("r") as fi:
            records = json.load(fi)

        self.root_hash = prolly_create(
            items=records, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )

    def test_get_leftmost_leaf(self):
        result = get_leftmost_leaf(self.root_hash, self.internal_nodes)
        print(len(result))

    def test_get_next_leaf(self):
        cache = {}
        next_leaf = [get_root(self.root_hash, self.internal_nodes)] + get_leftmost_leaf(
            self.root_hash, self.internal_nodes, cache
        )
        while next_leaf is not None:
            next_leaf = get_next_leaf(next_leaf, self.internal_nodes, cache)
