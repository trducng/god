from pathlib import Path


def init(working_dir: str, force: bool):
    """Setup configurations.

    Run this command to create:
        - untracked personal configuration
        - untracked plugin configuration
        - tracked shared configuration
        - tracked plugin configuration
    """
    # untracked
    Path(working_dir, "untracks", "bin").mkdir(exist_ok=force, parents=True)
