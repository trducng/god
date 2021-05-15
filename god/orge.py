import re
import sqlite3
from pathlib import Path

from constants import BASE_DIR, GOD_DIR, HASH_DIR, MAIN_DIR, DB_DIR, ORGE_DIR
from logs import get_state_ops


TYPE1 = {
    'NAME': 'index',
    'PATTERN': 'train1/(?P<label>cat|dog)\.(?P<id>\d+)\..+$',
    'PATH': 'path'
}

TYPE2 = {
    'NAME': 'index',
    'PATTERN': '(?P<id>.+?)(_(?P<masktype_>GT\d))?.[^.]+$',
    'PATH': {
        'group': 'masktype_',
        'conversion': {
            None: 'input',
            'GT0': 'horizontal_mask',
            'GT1': 'vertical_mask',
            'GT2': 'area_mask',
            'GT3': 'stamp_mask',
            'GT4': 'other_mask'
        }
    },
    'EXTRA_COLUMNS': {
        'features': 'text'
    }
}

TYPE_3_4 = {
    'NAME': 'index',
    'PATTERN': "train/(?P<class>.+?)/(?P<id>.+\d+)(?P<switch_>.pbdata|/geometry.pbdata|/video.MOV)$",
    'PATH': {
        'group': 'switch_',
        'conversion': {
            '.pbdata': 'location',
            '/geometry.pbdata': '3d_mask',
            '/video.MOV': 'input'
        },
    }
}


def create_db(config):
    con = sqlite3.connect(str(Path(ORGE_DIR, config['NAME'])))
    cur = con.cursor()

    pattern = re.compile(config['PATTERN'])

    # get columns from pattern
    groups = list(pattern.groupindex.keys())
    cols = [each for each in groups if each[-1] != '_']

    # get columns from PATH
    if isinstance(config['PATH'], str):
        cols += [config['PATH'], config['PATH'] + '_hash']
    else:
        path_cols = list(config['PATH']['conversion'].values())
        for _ in path_cols:
            cols += [_, _ + '_hash']

    # get columns from EXTRA_COLUMNS
    if config['EXTRA_COLUMNS']:
        cols += list(config['EXTRA_COLUMNS'].keys())

    return cols


    # sql = [f"{name} {attr['type']}" for (name, attr) in  TABLE_DEF['COLUMNS'].items()]
    # sql = ', '.join(sql)
    # sql = f'CREATE TABLE main({sql})'

    # cur.execute(sql)
    # con.commit()
    # con.close()


def construct_sql_logs():
    items = get_state_ops('.')
    pattern = re.compile(TABLE_DEF['ID'])

    # TODO basically everything is add here
    sql_logs = []
    for each_name, each_hash in items:
        cols = {}
        result = pattern.match(each_name)  #TODO HERE
        if not result:
            continue

        result_dict = result.groupdict()
        if 'id' not in result_dict:
            continue

        # TODO: should check if this is a add / edit / remove, for now let assume
        # it is add for simplicity.
        # INSERT INTO main(id, path, hash, label) VALUES("{id}", "{path}", "{hash}", "{label}")

        result_dict['hash'] = each_hash
        result_dict['path'] = each_name
        id_, path = result_dict['id'], result_dict['path']
        hash_, label = result_dict['hash'], result_dict['label']

        sql_log = f'INSERT INTO main(id, path, hash, label) VALUES("{id_}", "{path}", "{hash_}", "{label}")'
        sql_logs.append(sql_log)

    return sql_logs



def populate_db_from_sql_logs(sql_logs):

    con = sqlite3.connect(str(Path(ORGE_DIR, NAME)))
    cur = con.cursor()

    for each_statement in sql_logs:
        cur.execute(each_statement)

    con.commit()
    con.close()


if __name__ == '__main__':
    # sql_logs = construct_sql_logs()
    result = create_db(TYPE2)
    import pdb; pdb.set_trace()
    # populate_db_from_sql_logs(sql_logs)
