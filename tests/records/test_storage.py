"""Test storage's prolly tree implementation"""
import json
import shutil
import unittest
from pathlib import Path

from god.records.storage import (
    construct_internal_node,
    construct_leaf_node,
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

    def test_construct_leaf_node(self):
        with Path(self.example_json).open("r") as fi:
            records = json.load(fi)

        result = construct_leaf_node(records, self.cache_dir)
        exp_hash = "2c013e44868c0f85370ffc844d74f1761361477698c052e26cf250c8ed389e18"

        # check if same hash
        self.assertEqual(result, exp_hash)

        # check if same content in same order
        with (self.cache_dir / result).open("r") as fi:
            content = json.load(fi)
        self.assertEqual(content, records)

    def test_construct_internal_node(self):
        nodes = [
            [
                "b389cee9a3c33340f4af1e17dd4c0148e01028657cc287c66636a22d2a3bc558",
                "a20f3f1d8ad245e48ba1d75dc6a50284",
                "e2ef13332c23414487260f8aaa8a5aed",
            ],
            [
                "e8bee5c618a1a894de8482ebcf9bcdaf97e8f816ca1aa1b43f67fb0a5fb576ea",
                "e2ef38f2701648d9bdd21cb29131cef3",
                "fffe949024574adfa52cfdbe3386f25f",
            ],
        ]

        result = construct_internal_node(nodes, self.cache_dir)
        exp_hash = "7284479242768496b94a8a4d25cfaff9261db920e25652ccd1b9840fbe6914d1"

        # check if same hash
        self.assertEqual(result, exp_hash)

        # check if same content in same order
        with (self.cache_dir / result).open("r") as fi:
            content = json.load(fi)
        self.assertEqual(content, nodes)

    def test_prolly_create(self):
        """Test can create prolly"""
        with Path(self.example_json).open("r") as fi:
            records = json.load(fi)

        result = prolly_create(
            records=records, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )

        # should have expected root hash value
        exp_root = "d02b40c35b1630aa844ab97d2b1e6da6d9a0ca1f2704e50cbc9440821628b85d"
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
        result = {}
        for leaf_node in self.leaf_nodes.glob("*"):
            with leaf_node.open("r") as fi:
                result.update(json.load(fi))

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
            records=records, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
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
            keys = sorted(list(content.keys()))
            self.assertEqual(start_key, keys[0])
            self.assertEqual(stop_key, keys[-1])

    def test_get_records(self):
        """This test can also validate if `prolly_create` is correct"""
        result = get_records(
            root=self.root_hash, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )

        # check if containing the same items
        with Path(self.example_json).open("r") as fi:
            records = json.load(fi)

        self.assertEqual(len(result), len(records))
        self.assertEqual(result, records)

    def test_get_matching_child(self):
        node = "e8bee5c618a1a894de8482ebcf9bcdaf97e8f816ca1aa1b43f67fb0a5fb576ea"
        with (self.internal_nodes / node).open("r") as fi:
            content = json.load(fi)

        # smallest than anything should return the first child hash
        result = get_matching_child("e2ef38f2701648d9bdd21cb29131cef2", content)
        self.assertEqual(
            result, "9928cce0c1d5bceadc7bca514051c91491962428b8ec4de6516a41b5dd394330"
        )

        # smallest-equal should return the first child hash
        result = get_matching_child("e7a51ed7cf8f4932ad0e28ae555fde20", content)
        self.assertEqual(
            result, "9928cce0c1d5bceadc7bca514051c91491962428b8ec4de6516a41b5dd394330"
        )

        # middle exact match should return the matching child hash
        result = get_matching_child("fd24d94a2c71423f8197b85f71762a99", content)
        self.assertEqual(
            result, "88727cdea7e53d80d18609af18f744dcbd4b17773bf9589fc86d13392b69b258"
        )

        # middle should return matching child hash
        result = get_matching_child("fc24d94a2c71423f8197b85f71762a99", content)
        self.assertEqual(
            result, "88727cdea7e53d80d18609af18f744dcbd4b17773bf9589fc86d13392b69b258"
        )

        # largest than anything should return the last child hash
        result = get_matching_child("fffe949024574adfa52cfdbe3386f26f", content)
        self.assertEqual(
            result, "b4bf91a8d9cb664a6eef43e81c158f7c6c04c5f6f55efff77b5e6211c2d4e15d"
        )

        # largest-equal should return the last child hash
        result = get_matching_child("fffe949024574adfa52cfdbe3386f25f", content)
        self.assertEqual(
            result, "b4bf91a8d9cb664a6eef43e81c158f7c6c04c5f6f55efff77b5e6211c2d4e15d"
        )

    # def test_get_keys_indices(self):
    #     leaf = "3f7c39309d3cf7c5465458919c70e070d31edf4f35e71014826305b4c05e622f"
    #     with (self.leaf_nodes / leaf).open("r") as fi:
    #         content = json.load(fi)

    #     keys_indices = {
    #         "a2291ba1ee324aa5a2e7538639398849": 24,
    #         "b32cda9e7ed0479e8adabcfe3d540816": 111,
    #         "a20f3f1d8ad245e48ba1d75dc6a50284": 0,
    #     }

    #     result = get_keys_indices(keys=list(keys_indices.keys()), records=content)
    #     self.assertEqual(result, keys_indices)

    # def test_get_keys_values(self):
    #     leaf = "3f7c39309d3cf7c5465458919c70e070d31edf4f35e71014826305b4c05e622f"
    #     with (self.leaf_nodes / leaf).open("r") as fi:
    #         content = json.load(fi)

    #     keys_values = {
    #         "a20f3f1d8ad245e48ba1d75dc6a50284": {
    #             "number": None,
    #             "position": ["rect", 1190, 1641, 32, 24],
    #             "text": "Jean Everhardt",
    #         },
    #         "b32cda9e7ed0479e8adabcfe3d540816": {
    #             "number": 0.9656584316991396,
    #             "position": ["rect", 1089, 1121, 202, 30],
    #             "text": "James Dale",
    #         },
    #         "b30a47da68144508a663aa21da1ceb4c": {
    #             "number": 0.9875519230779324,
    #             "position": ["rect", 697, 311, 287, 32],
    #             "text": "Ralph Moss",
    #         },
    #     }

    #     result = get_keys_values(keys=list(keys_values.keys()), records=content)
    #     self.assertEqual(result, keys_values)

    def test_get_paths_to_records(self):
        keys_paths = {
            "874da66c02824e4ab44ca82245b181b1": [
                "d02b40c35b1630aa844ab97d2b1e6da6d9a0ca1f2704e50cbc9440821628b85d",
                "ae66e8d2be4f6507bce60157e5c8e3a2baf6fad84be2590763a319b7e676373d",
                "0e169b4c1dc5c6613ca13b35232c6527998681589b26e6d22c15cbdd36119740",
                "cf986c707021fa108cd0bfe924ef84e64698d395aac1f41a12f53be4752df283",
            ],
            "944a81e3ea924f7aa5611fbdc1664087": [
                "d02b40c35b1630aa844ab97d2b1e6da6d9a0ca1f2704e50cbc9440821628b85d",
                "ae66e8d2be4f6507bce60157e5c8e3a2baf6fad84be2590763a319b7e676373d",
                "0e169b4c1dc5c6613ca13b35232c6527998681589b26e6d22c15cbdd36119740",
                "cf986c707021fa108cd0bfe924ef84e64698d395aac1f41a12f53be4752df283",
            ],
            "a20f3f1d8ad245e48ba1d75dc6a50284": [
                "d02b40c35b1630aa844ab97d2b1e6da6d9a0ca1f2704e50cbc9440821628b85d",
                "7284479242768496b94a8a4d25cfaff9261db920e25652ccd1b9840fbe6914d1",
                "b389cee9a3c33340f4af1e17dd4c0148e01028657cc287c66636a22d2a3bc558",
                "3f7c39309d3cf7c5465458919c70e070d31edf4f35e71014826305b4c05e622f",
            ],
            "b32cda9e7ed0479e8adabcfe3d540816": [
                "d02b40c35b1630aa844ab97d2b1e6da6d9a0ca1f2704e50cbc9440821628b85d",
                "7284479242768496b94a8a4d25cfaff9261db920e25652ccd1b9840fbe6914d1",
                "b389cee9a3c33340f4af1e17dd4c0148e01028657cc287c66636a22d2a3bc558",
                "3f7c39309d3cf7c5465458919c70e070d31edf4f35e71014826305b4c05e622f",
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

        updated_root = prolly_insert(
            records=new_records,
            root=self.root_hash,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )
        # check for matching root hash
        self.assertEqual(
            updated_root,
            "00b07eab9674ac2444dc217b00c58a4a763567ef3b4efe07394ef1d29219e919",
        )

        # check for correct new records
        retrieved_records = prolly_locate(
            keys=list(new_records.keys()),
            root=updated_root,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )
        self.assertEqual(retrieved_records, new_records)

        # check for correct total records
        retrieved_records = get_records(
            root=updated_root, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )
        with self.example_json.open("r") as fi:
            original_records = json.load(fi)
            original_records.update(new_records)

        self.assertEqual(retrieved_records, original_records)

    def test_prolly_update(self):
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
        updated_root = prolly_update(
            records=updated_records,
            root=self.root_hash,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )

        # check for matching root hash
        self.assertEqual(
            updated_root,
            "9fa73c683d73963398472b4cc813643f659602ab4b19be6b708e79e9bcca3c95",
        )

        with self.example_json.open("r") as fi:
            original_records = json.load(fi)

        retrieved_records = get_records(
            root=updated_root, tree_dir=self.internal_nodes, leaf_dir=self.leaf_nodes
        )

        # check for not equal with original list of contents
        self.assertNotEqual(retrieved_records, original_records)

        # check for equal with modified list of contents
        for key, value in updated_records.items():
            original_records[key].update(value)
        self.assertEqual(retrieved_records, original_records)

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
            "016708b70ff819630ec34871816e065498f63d7ce017b409e30fbe07f78fe5f9",
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

        # assert matching root node
        self.assertEqual(
            updated_root,
            "6d67f6fb1864219315ea146fb63f6467fbeaaf7597f60fb2bed427ca534015d8",
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
        retrieved_records = prolly_locate(
            keys=list(updated_records.keys()),
            root=updated_root,
            tree_dir=self.internal_nodes,
            leaf_dir=self.leaf_nodes,
        )
        for key, value in retrieved_records.items():
            for col_name, col_value in updated_records[key].items():
                self.assertEqual(value[col_name], col_value)
