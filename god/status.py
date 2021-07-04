from god.branches.trackchanges import track_files


def status(fds, index_path, base_dir):
    """Track statuses of the directories

    # Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        base_dir <str>: project base directory
    """
    return track_files(fds, index_path, base_dir)
