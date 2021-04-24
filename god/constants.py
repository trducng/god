"""Temporary constants for development"""
from pathlib import Path


BASE_DIR = Path("/home/john/dogs-cats")
# BASE_DIR = "/home/john/datasets/imagenet/object_localization/temp"

GOD_DIR = Path(BASE_DIR,  ".god")
HASH_DIR = Path(GOD_DIR, "objects")
MAIN_DIR = Path(GOD_DIR, 'main')
LOG_DIR = Path(MAIN_DIR, 'logs')
DB_DIR = Path(MAIN_DIR, 'db')
MAIN_DB = 'main.db'
