"""
Commit the data for hashing
"""
from collections import defaultdict
from pathlib import Path

import yaml

from god.commits.base import calculate_commit_hash
from god.index.base import Index
from god.plugins.utils import installed_plugins, plugin_endpoints
from god.utils.common import get_string_hash


def save_dir(items: list, commit_dirs_dir: str) -> str:
    items = list(sorted(items, key=lambda obj: obj[0]))
    w = "\n".join(",".join(each) for each in items)
    h = get_string_hash(w)
    with Path(commit_dirs_dir, h).open("w") as fo:
        fo.write(w)
    return h


def expand_dir(dirs):
    result = dirs
    for each in dirs:
        result += [str(_) for _ in Path(each).parents]
    return list(set(result))


def store_dir(index_files_dirs: defaultdict, commit_dirs_dir):
    dirs = list(index_files_dirs.keys())
    dirs = expand_dir(dirs)
    h = ""
    for d in sorted(dirs, reverse=True):
        h = save_dir(index_files_dirs[d], commit_dirs_dir)
        if d != ".":
            index_files_dirs[str(Path(d).parent)].append((Path(d).name, "d", h))
    return h


def handle_one(name, commit_dirs_dir):
    index_path = plugin_endpoints(name)["index"]
    with Index(index_path) as index:
        files_info = index.get_folder(["."], get_remove=False)
        files_info = sorted(files_info, key=lambda obj: obj[0], reverse=True)

    index_files_dirs = defaultdict(list)
    for f in files_info:
        fp = Path(f[0])
        fh = f[2] or f[1]
        # can add exe bit here
        index_files_dirs[str(fp.parent)].append((fp.name, "f", fh))

    return store_dir(index_files_dirs, commit_dirs_dir)


def commit(user, email, message, prev_commit, commit_dir, commit_dirs_dir):
    """Commit from staging area

    # Args:
        user <str>: user name
        user_email <str>: user email address
        message <str>: commit message
        prev_commit <str>: previous commit id
        commit_dir <str>: path to store commit
        commit_dirs_dir <str>: path to store commit info

    # Returns:
        <str>: the commit hash
    """
    commit_obj = {
        "user": user,
        "email": email,
        "message": message,
        "prev": prev_commit,
        "core": {},
        "plugins": {},
    }

    commit_obj["core"]["files"] = handle_one("files", commit_dirs_dir)
    commit_obj["core"]["configs"] = handle_one("configs", commit_dirs_dir)
    commit_obj["core"]["plugins"] = handle_one("plugins", commit_dirs_dir)

    for plugin in installed_plugins():
        commit_obj["plugins"][plugin] = handle_one(plugin, commit_dirs_dir)

    # construct commit object
    # handle plugin
    commit_hash = calculate_commit_hash(commit_obj)
    commit_file = Path(commit_dir, commit_hash)
    # if commit_file.is_file():
    #     print("Commit already exists.")
    #     # TODO: require a stricter check for "objects" & "records" because the
    #     # commit hash will always be different because of diff in `prev_commit`
    #     return commit_hash

    with commit_file.open("w") as f_out:
        yaml.dump(commit_obj, f_out)

    commit_file.chmod(0o440)

    # reconstruct index
    for name in ["files", "configs", "plugins"] + installed_plugins():
        index_path = plugin_endpoints(name)["index"]
        with Index(index_path) as index:
            index.step()

    return commit_hash
