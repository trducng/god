import sqlite3
from constants import POINTER_FILE, DB_DIR


def get_current_db():
    if not POINTER_FILE.exists():
        return 'main.db'

    with POINTER_FILE.open('r') as f_in:
        current_db = f_in.read().splitlines()[0]

    if not current_db:
        return 'main.db'

    return current_db

def change_index(value):
    with POINTER_FILE.open('w') as f_out:
        f_out.write(value)

def get_history():
    """Print commit history of the data repo"""
    history = []
    current_db = get_current_db()
    while True:
        history.append(current_db)
        con = sqlite3.connect(str(DB_DIR / current_db))
        cur = con.cursor()
        current_db = cur.execute('SELECT hash FROM depend_on').fetchall()[0][0]
        con.close()
        if current_db == 'main.db':
            break

    for each_db in history:
        print(each_db)

    return history

if __name__ == '__main__':
    # result = get_current_db()
    get_history()
