import json
import sys

import click

from god.files.diff import diff
from god.utils.process import str_stdin_option


@click.group()
def main():
    pass


@main.group("hook")
def hook():
    pass


@hook.command("diff")
@click.option("--changes", "changes_in", type=str_stdin_option, default=sys.stdin)
def diff_cmd(changes_in):
    changes = json.loads(changes_in)
    diff(changes["add"], changes["update"], changes["remove"])


@hook.command("poststatus")
@click.option(
    "--files",
    type=str_stdin_option,
    default=sys.stdin,
    required=True,
    help="JSON format [[abspath1, hash1], [abspath2, hash2]...]",
)
def hook_poststatus_cmd(files):
    """Convert internal path to record name"""
    from god.files.hooks import poststatus

    file_status = json.loads(files)
    print(json.dumps(poststatus(file_status)))
