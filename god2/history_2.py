import sqlite3
from pathlib import Path

from god.base import get_current_commit_db, settings


def get_history():
    """Print commit history of the data repo"""
    history = []
    current_db = get_current_commit_db()
    while True:
        history.append(current_db)
        con = sqlite3.connect(str(Path(settings.DIR_DB, current_db)))
        cur = con.cursor()
        current_db = cur.execute("SELECT hash FROM depend_on").fetchall()[0][0]
        con.close()
        if not current_db:
            # reach the core of history
            break

    for each_db in history:
        print(each_db)

    return history


if __name__ == "__main__":
    # result = get_current_db()
    get_history()
