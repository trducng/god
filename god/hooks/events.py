import os
import subprocess

import click


def post_commit_hook():
    """Run this hook during post commit. Assume that it is the sqlite script"""
    subprocess.run(["god-db", "post-commit"])


def record_search_hook(name: str, queries: list, columns: list):
    """This hook is ran for searching result. Assume this is the sqlite script"""
    cmd = ["god-db", "search", name]
    for query in queries:
        cmd.append("--query")
        cmd.append(query)
    for column in columns:
        cmd.append("--col")
        cmd.append(column)

    completed_process = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return completed_process
