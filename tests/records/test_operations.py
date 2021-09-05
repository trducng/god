import unittest
from pathlib import Path
import shutil
import sys
import traceback
from pdb import Pdb

import pytest

from god.records.configs import RecordsConfig
from god.records.operations import path_to_record_id, parse, parse_strict, copy_tree
from god.records.storage import get_records
from god.utils.constants import RECORDS_INTERNALS, RECORDS_LEAVES
from god.utils.exceptions import RecordParsingError


class RecordsConfigTest(unittest.TestCase):
    def setUp(self):
        self.cache_dir = Path(".cache", "tests", "storage", "operations")
        if self.cache_dir.is_dir():
            shutil.rmtree(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.records_config_file = Path("tests", "assets", "records_config.yml")
        self.files = [
            "train1/dog.6522.jpg",
            "train1/dog.305.jpg",
            "train1/dog.4576.jpg",
            "train1/cat.2530.jpg",
            "train1/dog.520.jpg",
            "train1/computer.522.jpg",
            "test1/cat.525.jpg",
        ]

    def test_parse(self):
        config = RecordsConfig(
            records_name="type1", config_path=self.records_config_file
        )
        records = parse(self.files, config)
        self.assertEqual(len(records), 5)

        # force dup and no error raised
        records = parse(self.files + self.files, config)
        self.assertEqual(len(records), 5)

        # force dup and error raised
        with pytest.raises(RecordParsingError):
            records = parse_strict(self.files + self.files, config)  # force dup

    def test_path_to_record_id(self):
        config = RecordsConfig(
            records_name="type1", config_path=self.records_config_file
        )
        paths = path_to_record_id(self.files, config)
        self.assertEqual(
            paths,
            {
                "train1/cat.2530.jpg": "cat.2530",
                "train1/dog.305.jpg": "dog.305",
                "train1/dog.4576.jpg": "dog.4576",
                "train1/dog.520.jpg": "dog.520",
                "train1/dog.6522.jpg": "dog.6522",
            },
        )

    def test_copy_tree(self):
        source = Path("tests", "assets", "rud_bu")
        target = self.cache_dir / "copy_tree"
        (target / RECORDS_INTERNALS).mkdir(parents=True, exist_ok=True)
        (target / RECORDS_LEAVES).mkdir(parents=True, exist_ok=True)
        root = "d02b40c35b1630aa844ab97d2b1e6da6d9a0ca1f2704e50cbc9440821628b85d"
        copy_tree(root, source ,target)

        # shouldn't be equal as the records are modified multiple times so the root
        # does not correspond to all nodes
        self.assertNotEqual(
            len(list((target / RECORDS_INTERNALS).glob('*'))),
            len(list((source / RECORDS_INTERNALS).glob('*')))
        )

        self.assertNotEqual(
            len(list((target / RECORDS_LEAVES).glob('*'))),
            len(list((source / RECORDS_LEAVES).glob('*')))
        )

        # should have the same amount of records
        source_records = get_records(
            root=root,
            tree_dir=source / RECORDS_INTERNALS,
            leaf_dir=source / RECORDS_LEAVES
        )
        target_records = get_records(
            root=root,
            tree_dir=target / RECORDS_INTERNALS,
            leaf_dir=target / RECORDS_LEAVES
        )
        self.assertEqual(target_records, source_records)
