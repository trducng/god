"""Temporary constants for development"""
from pathlib import Path


BASE_DIR = "/home/john/datasets/dogs-cats"
GOD_DIR = Path(BASE_DIR,  ".god")
HASH_DIR = Path(GOD_DIR, "objects")
MAIN_DIR = Path(GOD_DIR, 'main')
