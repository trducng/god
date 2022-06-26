"""
Commit the data for hashing
"""
import os
import tempfile
from collections import defaultdict
from pathlib import Path

import yaml

from god.commits.base import calculate_commit_hash
from god.core.common import plugin_endpoints
from god.index.base import Index
from god.plugins.utils import installed_plugins
from god.utils.common import get_string_hash
from god.utils.process import communicate


def save_dir(items: list) -> str:
    items = list(sorted(items, key=lambda obj: obj[0]))
    w = "\n".join(",".join(each) for each in items)
    h = get_string_hash(w)

    fd, temp_path = tempfile.mkstemp()
    with open(temp_path, "w") as fo:
        fo.write(w)
    print("Dirs", temp_path, h)
    communicate(command=["god", "storages", "store-dirs"], stdin=[[temp_path, h]])
    os.close(fd)
    os.unlink(temp_path)
    return h


def expand_dir(dirs):
    result = dirs
    for each in dirs:
        result += [str(_) for _ in Path(each).parents]
    return list(set(result))


def store_dir(index_files_dirs: defaultdict):
    dirs = list(index_files_dirs.keys())
    dirs = expand_dir(dirs)
    h = ""
    for d in sorted(dirs, reverse=True):
        h = save_dir(index_files_dirs[d])
        if d != ".":
            index_files_dirs[str(Path(d).parent)].append((Path(d).name, "d", h))
    return h


def handle_one(name):
    index_path = plugin_endpoints(name)["index"]
    with Index(index_path) as index:
        files_info = index.get_folder(["."], get_remove=False, conflict=False)
        files_info = sorted(files_info, key=lambda obj: obj[0], reverse=True)

    index_files_dirs = defaultdict(list)
    for f in files_info:
        fp = Path(f[0])
        fh = f[2] or f[1]
        # can add exe bit here
        index_files_dirs[str(fp.parent)].append((fp.name, "f", fh))

    return store_dir(index_files_dirs)


def commit(user, email, message, prev_commit):
    """Commit from staging area

    # Args:
        user <str>: user name
        user_email <str>: user email address
        message <str>: commit message
        prev_commit <str>: previous commit id

    # Returns:
        <str>: the commit hash
    """
    commit_obj = {
        "user": user,
        "email": email,
        "message": message,
        "prev": prev_commit,
        "tracks": {},
    }

    commit_obj["tracks"]["files"] = handle_one("files")
    commit_obj["tracks"]["configs"] = handle_one("configs")
    commit_obj["tracks"]["plugins"] = handle_one("plugins")

    for plugin in installed_plugins():
        commit_obj["tracks"][plugin] = handle_one(plugin)

    # construct commit object
    # handle plugin
    commit_hash = calculate_commit_hash(commit_obj)
    # commit_file = Path(commit_dir, commit_hash)
    # # if commit_file.is_file():
    # #     print("Commit already exists.")
    # #     # TODO: require a stricter check for "objects" & "records" because the
    # #     # commit hash will always be different because of diff in `prev_commit`
    # #     return commit_hash

    # with commit_file.open("w") as f_out:
    #     yaml.dump(commit_obj, f_out)

    # commit_file.chmod(0o440)

    fd, temp_path = tempfile.mkstemp()
    with open(temp_path, "w") as fo:
        yaml.dump(commit_obj, fo)
    print("Commits", temp_path, commit_hash)
    communicate(
        command=["god", "storages", "store-commits"], stdin=[[temp_path, commit_hash]]
    )
    os.close(fd)
    os.unlink(temp_path)

    # reconstruct index
    for name in ["files", "configs", "plugins"] + installed_plugins():
        index_path = plugin_endpoints(name)["index"]
        with Index(index_path) as index:
            index.step()

    return commit_hash
