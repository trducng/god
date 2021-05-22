"""Base functions and constants. Helpful for other functions to build up."""
from pathlib import Path


GOD_DIR = ".god"

OBJ_DIR = f"{GOD_DIR}/objects"
MAIN_DIR = f"{GOD_DIR}/main"

LOG_DIR = f"{MAIN_DIR}/logs"
DB_DIR = f"{MAIN_DIR}/db"
POINTER_FILE = f"{MAIN_DIR}/pointers"
CACHE_DIR = f"{MAIN_DIR}/cache"



def get_base_dir(path):
    """Get `god` base dir from `path`

    # Args
        path <str>: the directory

    # Returns
        <str>: the directory that contains `.god` directory
    """
    current_path = Path(path).resolve()
    must_exist = [GOD_DIR, OBJ_DIR, MAIN_DIR]

    while True:
        fail = False
        for each_must in must_exist:
            if not (current_path / each_must).exists():
                fail = True
                break

        if fail:
            if current_path.parent == current_path:
                # this is root directory
                raise RuntimeError("Unitialized god repo. Please run `got init`")
            current_path = current_path.parent

        else:
            return str(current_path)


if __name__ == '__main__':
    print(get_base_dir('/home/john/datasets/god-test/type4/bike/batch-0'))
