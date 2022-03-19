import shutil
import subprocess
import uuid
from pathlib import Path

from god.core.common import get_base_dir


def edit_file(file_path: str):
    """Edit file, copy to temporary location and then move back"""
    filename = Path(file_path).name
    temp = str(Path(get_base_dir(), ".god", "temp", f"{filename}_{uuid.uuid1().hex}"))
    shutil.copy(file_path, temp)

    subprocess.run(["vim", temp])
    shutil.copy(temp, file_path)
    Path(temp).unlink()
