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
    transform_dict,
)
from god.records.storage import prolly_create, prolly_edit


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
            records=records, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )

    def test_get_leftmost_leaf(self):
        result = get_leftmost_leaf(self.root_hash, self.internal_nodes)
        result = result[-1]

        # manually retrieve the leftmost leaf
        min_hash, min_start_key, min_end_key = None, None, None
        for leaf in self.leaf_nodes.glob("*"):
            with leaf.open("r") as fi:
                records = json.load(fi)
            keys = list(records.keys())
            start_key, end_key = min(keys), max(keys)
            if min_end_key is None or end_key < min_end_key:
                min_hash = leaf.name
                min_start_key = start_key
                min_end_key = end_key

        self.assertEqual(result, [min_hash, min_start_key, min_end_key])

    def test_get_next_leaf(self):
        cache = {}
        leaves = []  # containing leaf hashes
        next_leaf = [get_root(self.root_hash, self.internal_nodes)] + get_leftmost_leaf(
            self.root_hash, self.internal_nodes, cache
        )
        leaves.append(next_leaf[-1][0])
        while next_leaf is not None:
            next_leaf = get_next_leaf(next_leaf, self.internal_nodes, cache)
            if next_leaf is not None:
                leaves.append(next_leaf[-1][0])

        # should have all correct leaf hashes
        expected = sorted([_.name for _ in self.leaf_nodes.glob("*")])
        leaves = sorted(leaves)
        self.assertEqual(leaves, expected)

        # the cache should contains all the internal nodes
        self.assertEqual(len(cache), len(list(self.internal_nodes.glob("*"))))

    def test_next_sibling(self):
        parent = "ae66e8d2be4f6507bce60157e5c8e3a2baf6fad84be2590763a319b7e676373d"
        current_end_key = "65d24fb628524c4ba80c9ec0ef674b11"
        expected = [
            "297cb4b4c9842d8109b281276ac9af411bf5156c828a65eb9a93f2e90045d740",
            "65d3fdc8c4b9460b898eb2398a972341",
            "873c28b22df44104805380dbbccaae55",
        ]
        result = get_next_sibling(
            key=current_end_key, parent=parent, node_dir=self.internal_nodes
        )
        self.assertEqual(result, expected)

    def test_transform_dict(self):
        dict1 = {"col0": [0, 1, 2], "col1": "val1", "col2": 2}
        dict2 = {  # col0 maintain, col1 change, col2 delete, col3 add
            "col0": [0, 1, 2],
            "col1": "val2",
            "col3": None,
        }
        add, update, delete = transform_dict(dict1, dict2)
        self.assertEqual(add, {"col3": None})
        self.assertEqual(update, {"col1": ["val1", "val2"]})
        self.assertEqual(delete, {"col2": 2})

    def test_compare_leaves(self):
        new_records = {
            "106ff384f7294d86a8f830c4fbfffe6c": {
                "text": "new text1",
                "position": ["rect", 53, 912, "updated", 155, 27],
                "number": 13,
            },
        }
        _ = prolly_edit(
            root=self.root_hash,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
            insert=new_records,
        )

        leaf1 = "63a8ee21bcb6ea28ecd3478c6ed81a3461356e85ad8f760e9022d28dbf7ec143"
        leaf2 = "2998f305c75fe7ecef7824548dcd01133b27a1fd2330fcbf5d5edb36f8e31c63"
        add, update, remove = compare_leaves(
            leaves1=[leaf1], leaves2=[leaf2], leaf_dir=self.leaf_nodes
        )
        self.assertEqual(len(add), 1)
        self.assertEqual(len(update), 0)
        self.assertEqual(len(remove), 0)

    def test_compare_tree(self):
        # modify the tree as in test_storage.py::StorageRUDTest::test_prolly_edit
        deleted_keys = [
            "65d3fdc8c4b9460b898eb2398a972341",
            "375ea4ca485241d1971c3a16fdfeb268",
            "e7be9e01dc7f47b69607dbf31204df01",
        ]
        updated_records = {
            "a20f3f1d8ad245e48ba1d75dc6a50284": {
                "number": 0.1231,
                "text": "Updated Name",
            },
            "b32cda9e7ed0479e8adabcfe3d540816": {
                "number": None,
                "position": ["rect", 1089, 1121, 202, 30, "updated"],
            },
            "b30a47da68144508a663aa21da1ceb4c": {},  # no update
        }
        new_records = {
            "106ff384f7294d86a8f830c4fbfffe6c": {
                "text": "new text1",
                "position": ["rect", 53, 912, "updated", 155, 27],
                "number": 13,
            },
            "a733126211b84c529f02a4298b32a9d8": {
                "text": "new text2",
                "position": ["rect", 53, 912, "hahahaa"],
                "number": None,
            },
        }

        updated_root = prolly_edit(
            root=self.root_hash,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
            insert=new_records,
            update=updated_records,
            delete=deleted_keys,
        )

        add, update, remove = compare_tree(
            tree1=self.root_hash,
            tree2=updated_root,
            node_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )

        self.assertEqual(add, new_records)
        self.assertEqual(sorted(list(remove.keys())), sorted(deleted_keys))
        self.assertEqual(
            len(update), len(updated_records) - 1
        )  # only update 2 out of 3
