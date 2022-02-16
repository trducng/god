"""Prepare repository for commit

Behaviors:
    - Add files to staging area
    - Construct new record collections -> maybe need interactive
    - Delete record collections -> maybe need interactive
    - Update record collections -> maybe need interactive
    - Add/Update/Remove records to/from record collection
    -> We need to understand about the working area for records
"""
from god.files.add import add as files_add


def add(fds, index_path, dir_obj, base_dir, dir_cache_records, dir_records):
    """Add the files, directories & all records to staging area.

    Args:
        fds <str>: the directory to add (absolute path)
        index_path <str>: path to index file
        dir_obj <str>: the path to object directory
        base_dir <str>: project base directory
        dir_cache_records <str>: directory containing working records
        dir_records <str>: directory containing to-be-committed records
    """
    # @TODO: this should be a binary process, rather than a Python import module
    # @TODO: don't need index_path, dir_obj, base_dir.... The components can figure
    # out these information by themselves.
    # @TODO: should we supply the settings, or should we let the plugins figure out
    # the settings values?
    #   - It seems we have to expose the config as a component, like a plumbing-command
    # so that 3rd-party plugin can readily use it to extract information they need.
    # @TODO: you need to define some standard config parameters so that your code can
    # follow the same behavior across plugins.
    files_add(fds, index_path, dir_obj, base_dir, dir_cache_records, dir_records)
