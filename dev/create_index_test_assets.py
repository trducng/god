import hashlib
import random
import time
from pathlib import Path

from god.index.base import Index

OUTPUT_FOLDER = "../tests/assets/index"

# create
output_folder = Path(OUTPUT_FOLDER)
if not output_folder.is_dir():
    raise OSError(f"Please create or correct OUTPUT_FOLDER {OUTPUT_FOLDER}")
god_folder = Path(output_folder, ".god")
god_folder.mkdir(exist_ok=True)
index_file = str(god_folder / "files")
index = Index(index_path=index_file)
index.build()

files = [
    "file1",
    "file2",
    "folder1/file1",
    "folder1/file2",
    "folder2/file1",
    "folder2/file2",
    "folder3/file1",
    "folder3/file2",
    "folder3/foldera/file1",
    "folder3/folderb/file1",
]

# commit stage
original_files = {fn: f"{fn} Content" for fn in files}
add = []
for fn, content in original_files.items():
    time.sleep(2 * random.random())
    fp = Path(output_folder, fn)
    fp.parent.mkdir(exist_ok=True)
    with fp.open("w") as fo:
        fo.write(content)
    add.append([fn, hashlib.sha256(content.encode()).hexdigest(), fp.stat().st_mtime])

with Index(index_path=index_file) as index:
    index.add(items=add, staged=False)


# staging stage
staged_add = {
    "folder4/file1": "folder4/file1 Content",
    "folder4/file2": "folder4/file2 Content",
}
stage = []
for fn, content in staged_add.items():
    time.sleep(2 * random.random())
    fp = Path(output_folder, fn)
    fp.parent.mkdir(exist_ok=True)
    with fp.open("w") as fo:
        fo.write(content)
    stage.append([fn, hashlib.sha256(content.encode()).hexdigest(), fp.stat().st_mtime])

with Index(index_path=index_file) as index:
    index.add(items=stage, staged=True)

staged_update = {
    "file1": "file1 Change1",
    "folder2/file1": "folder2/file1 Change1",
    "folder2/file2": "folder2/file2 Change1",
}
stage = []
for fn, content in staged_update.items():
    time.sleep(2 * random.random())
    fp = Path(output_folder, fn)
    with fp.open("w") as fo:
        fo.write(content)
    stage.append([fn, hashlib.sha256(content.encode()).hexdigest(), fp.stat().st_mtime])

with Index(index_path=index_file) as index:
    index.update(items=stage)

staged_remove = ["folder3/file1"]
with Index(index_path=index_file) as index:
    index.delete(items=staged_remove, staged=True)

# working stage
working_files = {
    "folder2/file2": "folder2/file2 Change2",
    "folder4/file1": "folder4/file1 Change1",
    "folder4/file3": "folder4/file3 Content",
}
for fn, content in working_files.items():
    time.sleep(2 * random.random())
    fp = Path(output_folder, fn)
    fp.parent.mkdir(exist_ok=True)
    with fp.open("w") as fo:
        fo.write(content)
