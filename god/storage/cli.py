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
@click.option("--plugin", type=str, required=False)
@click.pass_context
def main(ctx, plugin):
    ctx.ensure_object(dict)
    ctx.obj["type"] = get_backend(plugin)
    ctx.obj["plugin"] = plugin


@main.command()
@click.pass_context
def migrate(ctx):
    pass


@main.command("use")
@click.pass_context
def use(ctx):
    pass


@main.command("get-files")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [[abspath1, hash1], [abspath2, hash2]...]",
)
@click.pass_context
def get_files(ctx, files):
    file_path, file_hash = zip(*json.loads(files))
    ctx.obj["type"].get_files(file_hash, file_path)


@main.command("store-files")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [[abspath1, hash1], [abspath2, hash2]...]",
)
@click.pass_context
def store_files(ctx, files):
    """Store files from working directory to god storage"""
    file_path, file_hash = zip(*json.loads(files))
    ctx.obj["type"].store_files(file_path, file_hash)


@main.command("exists")
@click.option(
    "--files",
    type=str_stdin,
    default=sys.stdin,
    required=True,
    help="JSON format [hash1, hash2...]",
)
@click.pass_context
def exists_cmd(ctx, files):
    result = ctx.obj["type"].exists(json.loads(files))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
