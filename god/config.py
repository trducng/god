"""Config-related functionalities in god

Configuration file follows YAML format.

There are 2 types of configs in god:
    - local config - local repo config set by the direct user
    - shared local config - shared local config that applies to all users
"""
import yaml


DEFAULT_CONFIG = {
    'OBJECTS': {
        'STORAGE': 'local',
    }
}


def write_config(path, config_dict):
    """Write the config out to YAML file

    # Args:
        path <str|Path>: the path to config file
        config_dict <{}>: the configuration
    """
    with open(path, 'w') as f_out:
        yaml.dump(config_dict, f_out)
