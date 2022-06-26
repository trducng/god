from god.configs import get_config_path


def get_remote_declaration_config_path(base_dir: str = None) -> str:
    """Return the remote declaration config path"""
    return get_config_path(plugin="configs", level="local", base_dir=base_dir)
