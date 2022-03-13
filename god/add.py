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


def add(fds, base_dir):
    """Add the files, directories & all records to staging area.

    Args:
        fds <str>: the directory to add (absolute path)
        base_dir <str>: project base directory
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
    files_add(fds, base_dir)
