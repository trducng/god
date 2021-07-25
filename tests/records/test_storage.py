"""Test storage's prolly tree implementation"""
import json
import shutil
import unittest
from pathlib import Path

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


class StorageCreateTest(unittest.TestCase):
    def setUp(self):
        self.example_json = Path("./tests/assets/storage_source.json")
        self.cache_dir = Path(".cache/tests/storage/create")
        self.internal_nodes = Path(self.cache_dir, "nodes")
        self.leaf_nodes = Path(self.cache_dir, "leaves")

        # clean up and create the cache
        if self.cache_dir.is_dir():
            shutil.rmtree(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # create sub-dirs
        self.internal_nodes.mkdir()
        self.leaf_nodes.mkdir()

    def test_construct_node(self):
        with Path(self.example_json).open("r") as fi:
            records = json.load(fi)

        result = construct_node(records, self.cache_dir)
        exp_hash = "7d873e54f6d4f31955b953264af67ec962aa5c7cac9a416e88d7fb195b4aeacc"

        # check if same hash
        self.assertEqual(result, exp_hash)

        # check if same content in same order
        with (self.cache_dir / result).open("r") as fi:
            content = json.load(fi)
        self.assertEqual(content, records)

    def test_build_tree_trunk(self):
        # can be validated by `test_prolly_create`, waiting for other use cases
        return

    def test_prolly_create(self):
        """Test can create prolly"""
        with Path(self.example_json).open("r") as fi:
            records = json.load(fi)

        result = prolly_create(
            items=records, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )

        # should have expected root hash value
        exp_root = "5767732041e94954e4fb93cbc49afe5a93e5f3a2e020ee374a6e7781b8b05251"
        self.assertEqual(result, exp_root)

        # should have the expected number of nodes
        exp_number_of_internal_nodes = 8
        self.assertEqual(
            len(list(self.internal_nodes.glob("*"))),
            exp_number_of_internal_nodes,
        )

        exp_number_of_leaf_nodes = 14
        self.assertEqual(
            len(list(self.leaf_nodes.glob("*"))),
            exp_number_of_leaf_nodes,
        )

        # should have same items
        result = []
        for leaf_node in self.leaf_nodes.glob("*"):
            with leaf_node.open("r") as fi:
                result += json.load(fi)

        result = sorted(result, key=lambda obj: list(obj.keys())[0])
        records = sorted(records, key=lambda obj: list(obj.keys())[0])

        self.assertEqual(len(result), len(records))
        self.assertEqual(result, records)


class StorageRUDTest(unittest.TestCase):
    """Test the data for RUD operations"""

    def setUp(self):
        self.example_json = Path("./tests/assets/storage_source.json")
        self.cache_dir = Path(".cache/tests/storage/rud")
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

    def test_get_leaf_nodes(self):
        leaf_nodes = get_leaf_nodes(
            root=self.root_hash, tree_dir=self.internal_nodes, sort_keys=True
        )

        # check if containing the same leaf nodes
        result = [leaf_node[0] for leaf_node in leaf_nodes]
        result = sorted(result)

        leaf_hashes = [leaf_node.name for leaf_node in self.leaf_nodes.glob("*")]
        leaf_hashes = sorted(leaf_hashes)
        self.assertEqual(result, leaf_hashes)

        # check if containing the correct boundary keys
        for leaf_hash, start_key, stop_key in leaf_nodes:
            with (self.leaf_nodes / leaf_hash).open("r") as fi:
                content = json.load(fi)
            self.assertEqual(start_key, list(content[0].keys())[0])
            self.assertEqual(stop_key, list(content[-1].keys())[0])

    def test_get_records(self):
        """This test can also validate if `prolly_create` is correct"""
        result = get_records(
            root=self.root_hash, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )

        # check if containing the same items
        with Path(self.example_json).open("r") as fi:
            records = json.load(fi)

        result = sorted(result, key=lambda obj: list(obj.keys())[0])
        records = sorted(records, key=lambda obj: list(obj.keys())[0])

        self.assertEqual(len(result), len(records))
        self.assertEqual(result, records)

    def test_get_matching_child(self):
        node = "d33a3bf6f0a83243ecd553c28d7a7aa1d11fed05f84584f5a7b795c008fa5a73"
        with (self.internal_nodes / node).open("r") as fi:
            content = json.load(fi)

        # smallest than anything should return the first child hash
        result = get_matching_child("e2ef38f2701648d9bdd21cb29131cef2", content)
        self.assertEqual(
            result, "2dabfc40889afe085a1431d2529b5c4beaf92cc886dcdb63dc3c76aa1f8732d0"
        )

        # smallest-equal should return the first child hash
        result = get_matching_child("e7a51ed7cf8f4932ad0e28ae555fde20", content)
        self.assertEqual(
            result, "2dabfc40889afe085a1431d2529b5c4beaf92cc886dcdb63dc3c76aa1f8732d0"
        )

        # middle exact match should return the matching child hash
        result = get_matching_child("fd24d94a2c71423f8197b85f71762a99", content)
        self.assertEqual(
            result, "7cc40ecb6b9bd08a711428373b1356ddbd222329f55da81928c2daa57b5d71c1"
        )

        # middle should return matching child hash
        result = get_matching_child("fc24d94a2c71423f8197b85f71762a99", content)
        self.assertEqual(
            result, "7cc40ecb6b9bd08a711428373b1356ddbd222329f55da81928c2daa57b5d71c1"
        )

        # largest than anything should return the last child hash
        result = get_matching_child("fffe949024574adfa52cfdbe3386f26f", content)
        self.assertEqual(
            result, "6654cf21c7aaccc7bdf5f4e383ce4af2e902dc0420af3399db991e6d220001c1"
        )

        # largest-equal should return the last child hash
        result = get_matching_child("fffe949024574adfa52cfdbe3386f25f", content)
        self.assertEqual(
            result, "6654cf21c7aaccc7bdf5f4e383ce4af2e902dc0420af3399db991e6d220001c1"
        )

    def test_get_keys_indices(self):
        leaf = "da7c5bf01ce8823e23fd07e4f1b9e6bc08e164867aceda9e73d27a9b80e49bd3"
        with (self.leaf_nodes / leaf).open("r") as fi:
            content = json.load(fi)

        keys_indices = {
            "a2291ba1ee324aa5a2e7538639398849": 24,
            "b32cda9e7ed0479e8adabcfe3d540816": 111,
            "a20f3f1d8ad245e48ba1d75dc6a50284": 0,
        }

        result = get_keys_indices(keys=list(keys_indices.keys()), records=content)
        self.assertEqual(result, keys_indices)

    def test_get_keys_values(self):
        leaf = "da7c5bf01ce8823e23fd07e4f1b9e6bc08e164867aceda9e73d27a9b80e49bd3"
        with (self.leaf_nodes / leaf).open("r") as fi:
            content = json.load(fi)

        keys_values = {
            "a20f3f1d8ad245e48ba1d75dc6a50284": {
                "number": None,
                "position": ["rect", 1190, 1641, 32, 24],
                "text": "Jean Everhardt",
            },
            "b32cda9e7ed0479e8adabcfe3d540816": {
                "number": 0.9656584316991396,
                "position": ["rect", 1089, 1121, 202, 30],
                "text": "James Dale",
            },
            "b30a47da68144508a663aa21da1ceb4c": {
                "number": 0.9875519230779324,
                "position": ["rect", 697, 311, 287, 32],
                "text": "Ralph Moss",
            },
        }

        result = get_keys_values(keys=list(keys_values.keys()), records=content)
        self.assertEqual(result, keys_values)

    def test_get_paths_to_records(self):
        keys_paths = {
            "874da66c02824e4ab44ca82245b181b1": [
                "5767732041e94954e4fb93cbc49afe5a93e5f3a2e020ee374a6e7781b8b05251",
                "c2fcfdeef86d41814271908512c767d693e6fc29167cd0f2e5805e3cbcdb518b",
                "97e8c479e173e77852885c975464f7a4be4fcb98abd932de57fe0ca32a8b3ba8",
                "5d5631b095e62add9bd6b18966c0aaf03e620785654bbd73a7106cffe2e336d1",
            ],
            "944a81e3ea924f7aa5611fbdc1664087": [
                "5767732041e94954e4fb93cbc49afe5a93e5f3a2e020ee374a6e7781b8b05251",
                "c2fcfdeef86d41814271908512c767d693e6fc29167cd0f2e5805e3cbcdb518b",
                "97e8c479e173e77852885c975464f7a4be4fcb98abd932de57fe0ca32a8b3ba8",
                "5d5631b095e62add9bd6b18966c0aaf03e620785654bbd73a7106cffe2e336d1",
            ],
            "a20f3f1d8ad245e48ba1d75dc6a50284": [
                "5767732041e94954e4fb93cbc49afe5a93e5f3a2e020ee374a6e7781b8b05251",
                "b8c23a56553372fa4db10a59c27cdc9bc818d2989133f2d8199f9ce9e65953c0",
                "f3bca04d81821946f09088aeff5490684b6619aa05d6f4d18c58bd9fc28a9b45",
                "da7c5bf01ce8823e23fd07e4f1b9e6bc08e164867aceda9e73d27a9b80e49bd3",
            ],
            "b32cda9e7ed0479e8adabcfe3d540816": [
                "5767732041e94954e4fb93cbc49afe5a93e5f3a2e020ee374a6e7781b8b05251",
                "b8c23a56553372fa4db10a59c27cdc9bc818d2989133f2d8199f9ce9e65953c0",
                "f3bca04d81821946f09088aeff5490684b6619aa05d6f4d18c58bd9fc28a9b45",
                "da7c5bf01ce8823e23fd07e4f1b9e6bc08e164867aceda9e73d27a9b80e49bd3",
            ],
        }

        result = get_paths_to_records(
            keys=list(keys_paths.keys()),
            root=self.root_hash,
            tree_dir=self.internal_nodes,
        )
        self.assertEqual(result, keys_paths)

    def test_prolly_locate(self):
        keys_values = {
            "a20f3f1d8ad245e48ba1d75dc6a50284": {
                "number": None,
                "position": ["rect", 1190, 1641, 32, 24],
                "text": "Jean Everhardt",
            },
            "b32cda9e7ed0479e8adabcfe3d540816": {
                "number": 0.9656584316991396,
                "position": ["rect", 1089, 1121, 202, 30],
                "text": "James Dale",
            },
            "b30a47da68144508a663aa21da1ceb4c": {
                "number": 0.9875519230779324,
                "position": ["rect", 697, 311, 287, 32],
                "text": "Ralph Moss",
            },
        }

        result = prolly_locate(
            keys=list(keys_values.keys()),
            root=self.root_hash,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )
        self.assertEqual(result, keys_values)

    def test_prolly_insert(self):
        new_records = [
            {
                "106ff384f7294d86a8f830c4fbfffe6c": {
                    "text": "new text1",
                    "position": ["rect", 53, 912, "updated", 155, 27],
                    "number": 13,
                },
            },
            {
                "a733126211b84c529f02a4298b32a9d8": {
                    "text": "new text2",
                    "position": ["rect", 53, 912, "hahahaa"],
                    "number": None,
                }
            },
        ]
        new_records_dict = {list(_.keys())[0]: list(_.values())[0] for _ in new_records}

        updated_root = prolly_insert(
            records=new_records,
            root=self.root_hash,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )
        # check for matching root hash
        self.assertEqual(
            updated_root,
            "31625214eba7eec2c47c58530d3f418aa022f03a8d7bb81574d7cce3a87fbf2b",
        )

        # check for correct new records
        retrieved_records = prolly_locate(
            keys=list(new_records_dict.keys()),
            root=updated_root,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )
        self.assertEqual(retrieved_records, new_records_dict)

        # check for correct total records
        retrieved_records = get_records(
            root=updated_root, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )
        with self.example_json.open("r") as fi:
            original_records = json.load(fi) + new_records

        retrieved_records = sorted(
            retrieved_records, key=lambda obj: list(obj.keys())[0]
        )
        original_records = sorted(original_records, key=lambda obj: list(obj.keys())[0])
        self.assertEqual(retrieved_records, original_records)

    def test_prolly_update(self):
        updated_records = [
            {
                "a20f3f1d8ad245e48ba1d75dc6a50284": {
                    "number": 0.1231,
                    "text": "Updated Name",
                },
            },
            {
                "b32cda9e7ed0479e8adabcfe3d540816": {
                    "number": None,
                    "position": ["rect", 1089, 1121, 202, 30, "updated"],
                },
            },
            {"b30a47da68144508a663aa21da1ceb4c": {}},  # no update
        ]
        updated_records_dict = {
            list(_.keys())[0]: list(_.values())[0] for _ in updated_records
        }
        updated_root = prolly_update(
            records=updated_records,
            root=self.root_hash,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )

        # check for matching root hash
        self.assertEqual(
            updated_root,
            "f6a9c87c08920d513ec5e45982b05daca4875e7917d8f022381d988b56a53d97",
        )

        with self.example_json.open("r") as fi:
            original_records = json.load(fi)

        retrieved_records = get_records(
            root=updated_root, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )

        # check for not equal with original list of contents
        retrieved_records = sorted(
            retrieved_records, key=lambda obj: list(obj.keys())[0]
        )
        original_records = sorted(original_records, key=lambda obj: list(obj.keys())[0])
        self.assertNotEqual(retrieved_records, original_records)

        # check for equal with modified list of contents
        retrieved_records = sorted(
            retrieved_records, key=lambda obj: list(obj.keys())[0]
        )
        updated_keys = set(updated_records_dict.keys())
        original_records_new = []
        for record in original_records:
            key = list(record.keys())[0]
            if key in updated_keys:
                value = list(record.values())[0]
                value.update(updated_records_dict[key])
        original_records_new = sorted(
            original_records_new, key=lambda obj: list(obj.keys())[0]
        )
        self.assertNotEqual(retrieved_records, original_records_new)

    def test_prolly_delete(self):
        deleted_keys = [
            "65d3fdc8c4b9460b898eb2398a972341",
            "375ea4ca485241d1971c3a16fdfeb268",
            "e7be9e01dc7f47b69607dbf31204df01",
        ]
        updated_root = prolly_delete(
            keys=deleted_keys,
            root=self.root_hash,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )

        # check for matching root hash
        self.assertEqual(
            updated_root,
            "2ce21921bd92be458ad76e90ddd7d363f3126f12477dc2c16d02ea16bb0ecc27",
        )

        # check for retrieved records should be None
        records = prolly_locate(
            keys=deleted_keys,
            root=updated_root,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )
        for value in records.values():
            self.assertIs(value, None)

        # check for records should be removed
        with self.example_json.open("r") as fi:
            original_records = json.load(fi)

        retrieved_records = get_records(
            root=updated_root, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )

        self.assertEqual(
            len(retrieved_records), len(original_records) - len(deleted_keys)
        )

    def test_prolly_edit(self):
        deleted_keys = [
            "65d3fdc8c4b9460b898eb2398a972341",
            "375ea4ca485241d1971c3a16fdfeb268",
            "e7be9e01dc7f47b69607dbf31204df01",
        ]
        updated_records = [
            {
                "a20f3f1d8ad245e48ba1d75dc6a50284": {
                    "number": 0.1231,
                    "text": "Updated Name",
                },
            },
            {
                "b32cda9e7ed0479e8adabcfe3d540816": {
                    "number": None,
                    "position": ["rect", 1089, 1121, 202, 30, "updated"],
                },
            },
            {"b30a47da68144508a663aa21da1ceb4c": {}},  # no update
        ]
        new_records = [
            {
                "106ff384f7294d86a8f830c4fbfffe6c": {
                    "text": "new text1",
                    "position": ["rect", 53, 912, "updated", 155, 27],
                    "number": 13,
                },
            },
            {
                "a733126211b84c529f02a4298b32a9d8": {
                    "text": "new text2",
                    "position": ["rect", 53, 912, "hahahaa"],
                    "number": None,
                }
            },
        ]
        updated_root = prolly_edit(
            root=self.root_hash,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
            insert=new_records,
            update=updated_records,
            delete=deleted_keys,
        )

        # assert matching root node
        self.assertEqual(
            updated_root,
            "634353eb2c9e011212b71602e9e519f44c04eb3096850a7b6105c1a23502ebc7",
        )

        # assert correct number of records
        with self.example_json.open("r") as fi:
            original_records = json.load(fi)
        retrieved_records = get_records(
            root=updated_root, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )
        self.assertEqual(
            len(retrieved_records),
            len(original_records) + len(new_records) - len(deleted_keys),
        )

        # assert the updated items are correct
        updated_dict = {list(_.keys())[0]: list(_.values())[0] for _ in updated_records}
        retrieved_records = prolly_locate(
            keys=list(updated_dict.keys()),
            root=updated_root,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )
        for key, value in retrieved_records.items():
            for col_name, col_value in updated_dict[key].items():
                self.assertEqual(value[col_name], col_value)
