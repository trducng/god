import sqlite3
import re
from pathlib import Path


DB_PATH = str(Path('/home/john/datasets/dogs-cats/.god/index').resolve())
DB_NAME = 'main'
DB_SCHEMA = {
    # each file path is validated against column that has {}.
    # the regex is always be used to match with file name by default
    # can specify a file to match
    # this is hard. The software engineer in the company will not be able to
    # handle this
    'input': {
        'anchor': '^.+\.(\d+)\.[^.]+$',
        'field': '.*'
    },
    'label': '.+/(\S+)\.\d+.+',
    # will create a table 'features' with these values
    'features': ['blurry', 'hard-to-see']
}


def create_table():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    for field_name, filed_def in DB_SCHEMA.items():
        # create all ManyToMany table

        # create real table
        pass
