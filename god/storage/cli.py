import json
import sys
from io import TextIOWrapper

import click

from god.storage.commons import get_backend


def str_stdin(x) -> str:
    """click type check to support both `echo "string" | exe` and `exe --opt "string"`

    Args:
        x: the object passed from command line

    Returns:
        String representation
    """
    if isinstance(x, TextIOWrapper):
        return x.read().strip()

    if x is None:
        return ""

    return str(x)


@click.group()
@click.pass_context
def main(ctx):
    ctx.ensure_object(dict)
    ctx.obj["type"] = get_backend()


@main.command("migrate")
@click.argument("path")
@click.pass_context
def migrate_cmd(ctx, path):
    from god.storage.migrate import migrate

    new_storage = get_backend(path)
    old_storage = ctx.obj["type"]
    migrate(old_storage, new_storage)


@main.command("use")
@click.argument("config")
def use(config):
    from god.utils.constants import FILE_LINK

    with open(FILE_LINK, "r") as fi:
        data = json.load(fi)
    data["STORAGE"] = config
    with open(FILE_LINK, "w") as fo:
        json.dump(data, fo)


@main.command("get-objects")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [[abspath1, hash1], [abspath2, hash2]...]",
)
@click.pass_context
def get_objects(ctx, files):
    file_path, file_hash = zip(*json.loads(files))
    ctx.obj["type"].get_objects(file_hash, file_path)


@main.command("store-objects")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [[abspath1, hash1], [abspath2, hash2]...]",
)
@click.pass_context
def store_objects(ctx, files):
    """Store files from working directory to god storage"""
    file_path, file_hash = zip(*json.loads(files))
    ctx.obj["type"].store_objects(file_path, file_hash)


@main.command("have-objects")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [hash1, hash2...]",
)
@click.pass_context
def have_objects(ctx, files):
    result = ctx.obj["type"].have_objects(json.loads(files))
    print(json.dumps(result))


@main.command("get-dirs")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [[abspath1, hash1], [abspath2, hash2]...]",
)
@click.pass_context
def get_dirs(ctx, files):
    file_path, file_hash = zip(*json.loads(files))
    ctx.obj["type"].get_dirs(file_hash, file_path)


@main.command("store-dirs")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [[abspath1, hash1], [abspath2, hash2]...]",
)
@click.pass_context
def store_dirs(ctx, files):
    """Store files from working directory to god storage"""
    file_path, file_hash = zip(*json.loads(files))
    ctx.obj["type"].store_dirs(file_path, file_hash)


@main.command("have-dirs")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [hash1, hash2...]",
)
@click.pass_context
def have_dirs(ctx, files):
    result = ctx.obj["type"].have_dirs(json.loads(files))
    print(json.dumps(result))


@main.command("get-commits")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [[abspath1, hash1], [abspath2, hash2]...]",
)
@click.pass_context
def get_commits(ctx, files):
    file_path, file_hash = zip(*json.loads(files))
    ctx.obj["type"].get_commits(file_hash, file_path)


@main.command("store-commits")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [[abspath1, hash1], [abspath2, hash2]...]",
)
@click.pass_context
def store_commits(ctx, files):
    """Store files from working directory to god storage"""
    file_path, file_hash = zip(*json.loads(files))
    ctx.obj["type"].store_commits(file_path, file_hash)


@main.command("have-commits")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [hash1, hash2...]",
)
@click.pass_context
def have_commits(ctx, files):
    result = ctx.obj["type"].have_commits(json.loads(files))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
