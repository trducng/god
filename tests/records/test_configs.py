"""Test the records config"""
import unittest
from pathlib import Path
import sys
import traceback
from pdb import Pdb


from god.records.configs import RecordsConfig


class RecordsConfigTest(unittest.TestCase):
    def setUp(self):
        self.records_config_file = Path("./tests/assets/records_config.yml")

    def test_load_type1(self):
        config = RecordsConfig(
            records_name="type1", config_path=self.records_config_file
        )

        # test correct pattern
        self.assertEqual(
            config.get_pattern(),
            r"(?P<input>train1/(?P<id>(?P<label>cat|dog)\.\d+)\..+$)",
        )

        # test correct number of path columns
        self.assertEqual(config.get_path_columns(), ["input"])

        # test correct number of columns
        self.assertEqual(len(config.get_columns_and_types()[0]), 3)

    def test_load_type2(self):
        config = RecordsConfig(
            records_name="type2", config_path=self.records_config_file
        )

        # test correct pattern
        self.assertEqual(
            config.get_pattern(), r"(?P<id>.+?)(_(?P<masktype_>GT\d))?.[^.]+$"
        )

        # test correct number of path columns
        self.assertEqual(
            sorted(config.get_path_columns()),
            sorted(
                [
                    "input",
                    "horizontal_mask",
                    "vertical_mask",
                    "area_mask",
                    "stamp_mask",
                    "other_mask",
                ]
            ),
        )

        # test correct conversion group
        self.assertEqual(
            dict(config.get_group_rule()),
            {
                "masktype_": {
                    None: "input",
                    "GT0": "horizontal_mask",
                    "GT1": "vertical_mask",
                    "GT2": "area_mask",
                    "GT3": "stamp_mask",
                    "GT4": "other_mask",
                }
            },
        )

        # test correct number of columns
        self.assertEqual(len(config.get_columns_and_types()[0]), 8)

        # test correct primary columns
        self.assertEqual(len(config.get_primary_cols()), 6)

    def test_load_type3(self):
        config = RecordsConfig(
            records_name="type3", config_path=self.records_config_file
        )

        # test correct pattern
        self.assertEqual(config.get_pattern(), r"(?P<id>.+)\.(?P<switch_>.+$)")

        # test correct number of path columns
        self.assertEqual(
            sorted(config.get_path_columns()),
            sorted(["input", "label"]),
        )

        # test correct conversion group
        self.assertEqual(
            dict(config.get_group_rule()),
            {
                "switch_": {
                    "png": "input",
                    "jpeg": "input",
                    "jpg": "input",
                    "json": "label",
                }
            },
        )

        # test correct number of columns
        self.assertEqual(len(config.get_columns_and_types()[0]), 3)

        # test correct primary columns
        self.assertEqual(len(config.get_primary_cols()), 2)

    def test_load_type4(self):
        config = RecordsConfig(
            records_name="type4", config_path=self.records_config_file
        )

        # test correct pattern
        self.assertEqual(
            config.get_pattern(),
            r"(?P<class>.+?)/(?P<id>.+\d+)(?P<switch_>.pbdata|/geometry.pbdata|/video.MOV)$",
        )

        # test correct number of path columns
        self.assertEqual(
            sorted(config.get_path_columns()),
            sorted(["location", "mask_3d", "input"]),
        )

        # test correct conversion group
        self.assertEqual(
            dict(config.get_group_rule()),
            {
                "switch_": {
                    ".pbdata": "location",
                    "/geometry.pbdata": "mask_3d",
                    "/video.MOV": "input",
                }
            },
        )

        # test correct number of columns
        self.assertEqual(len(config.get_columns_and_types()[0]), 6)

        # test correct primary columns
        self.assertEqual(len(config.get_primary_cols()), 3)


if __name__ == "__main__":
    pdb = Pdb()
    try:
        records_config_file = Path("./tests/assets/records_config.yml")
        config = RecordsConfig(records_name="type1", config_path=records_config_file)
    except Exception:
        traceback.print_exc()
        print("Uncaught exception. Entering post mortem debugging")
        t = sys.exc_info()[2]
        pdb.interaction(None, t)
