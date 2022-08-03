import sys
from typing import Union


def show_download_progress(
    total_files: Union[int, None], total_bytes: Union[int, None]
):
    """Show download progress to stderr

    If both total_files and total_bytes are None, then it is the last item.

    Args:
        total_files: total number of downloaded files
        total_bytes: total amount of downloaded bytes
    """
    if total_files is None and total_bytes is None:
        print(file=sys.stderr)
    else:
        print(
            f"Downloaded {total_bytes} bytes for {total_files} files",
            end="\r",
            file=sys.stderr,
        )
