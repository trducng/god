from constants import POINTER_FILE


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

if __name__ == '__main__':
    result = get_current_db()
    print(result)
