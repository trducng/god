from pathlib import Path


def init(working_dir: str, force: bool):
    """Setup configurations.

    Run this command to create:
        - untracked personal configuration
        - tracked shared configuration
    """
    # untracked
    Path(working_dir, "untracks").mkdir(exist_ok=force, parents=True)

    # tracked
    Path(working_dir, "tracks").mkdir(exist_ok=force, parents=True)
