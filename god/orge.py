import re
import sqlite3
from pathlib import Path

from constants import BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR, DB_DIR, ORGE_DIR
from logs import get_state_ops


NAME = 'train'
TABLE_DEF = {
    'ID': 'train1/(?P<label>cat|dog)\.(?P<id>\d+)\..+$',
    'COLUMNS': {
        'id': {
            'group': 'id',
            'type': 'integer'
        },
        'path': {
            'type': 'text',
            'pattern': '.*'
        },
        'hash': {
            'type': 'text',
            'is_hash': True
        },
        'label': {
            'group': 'label',
            'type': 'text'
        }
    }
}


# 1. Construct comparisions
# 2. Construct logs -> should be SQL string
# 3. Create database
# 4. Populate the database

def create_db():
    con = sqlite3.connect(str(Path(ORGE_DIR, NAME)))
    cur = con.cursor()

    sql = [f"{name} {attr['type']}" for (name, attr) in  TABLE_DEF['COLUMNS'].items()]
    sql = ', '.join(sql)
    sql = f'CREATE TABLE main({sql})'

    cur.execute(sql)
    con.commit()
    con.close()


def construct_sql_logs():
    items = get_state_ops('.')
    pattern = re.compile(TABLE_DEF['ID'])

    for each_item in items:
        _id = re.match()#TODO HERE


def populate_db():
    con = sqlite3.connect(str(Path(ORGE_DIR, NAME)))
    cur = con.cursor()

    items = get_state_ops('.')
    pattern = re.compile(TABLE_DEF['ID'])


if __name__ == '__main__':
    create_db()
