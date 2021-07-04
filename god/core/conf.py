from dataclasses import dataclass
from pathlib import Path

import yaml

import god.utils.constants as c
from god.core.common import get_base_dir


def parse_dot_notation_to_dict(notation, value, upper=True):
    """Parse dot notation to dictionary

    Example:
        >> parse_dot_notation_to_dict('abc.xyz': 10)
        { "ABC": { "XYZ", 10 }}

    # Args:
        notation <str>: the dot notation
        value <Any>: value
        upper <bool>: whether to uppercase the key

    # Returns:
        <{}>: the parsed dictionary
    """
    components = notation.split(".")
    components = list(reversed(components))
    if upper:
        components = [_.upper() for _ in components]

    result = {components[0]: value}
    for item in components[1:]:
        result = {item: result}

    return result


def update_local_config(config_path, config_dict):
    """Write the config out to YAML file

    # Args:
        config_path <str|Path>: the path to config file
        config_dict <{}>: the configuration
    """
    settings = read_local_config(config_path)
    for key, value in config_dict.items():
        parsed_value = parse_dot_notation_to_dict(key, value)
        settings.set_values(**parsed_value)

    settings = settings.as_dict()
    with open(config_path, "w") as f_out:
        yaml.dump(settings, f_out)


def read_local_config(config_path):
    """Read values from local config into dictionary

    # Args:
        config_path <str>: path to config path

    # Returns:
        <Settings>: the setting object
    """
    settings = Settings()
    if Path(config_path).is_file():
        settings.set_values_from_yaml(config_path)
    return settings


@dataclass(frozen=True)
class Settings(object):
    """Global setting module.

    The `settings` object will be accessible for all classes and functions.

    This settings module looks for and constructs settings in this following priority:
        - User params
        - Project-level (at `.godconfig`)
        - User-level (at `~/.godconfig`)
        - System-level (at `/etc/godconfig`)

    # Args
        level <int>: as setting can be nested, this is the level of setting
    """

    _PARAM_ALLOWED_SETTINGS = ["DEBUG"]

    def __init__(self, level=0):
        """Initiate the setting object"""
        object.__setattr__(self, "_level", level)
        object.__setattr__(self, "_initialized", False)
        object.__setattr__(self, "values", [])

    def parse(self, item):
        """Parse the item"""
        result = item

        if isinstance(item, dict):
            result = Settings(level=self._level + 1)
            result.set_values(**item)
        elif isinstance(item, (list, tuple)):
            result = []
            for each_item in item:
                result.append(self.parse(each_item))
            result = tuple(result)

        return result

    def set_values(self, **kwargs):
        """Read settings"""
        if self._initialized:
            raise AttributeError("Setting has been initiated, cannot be re-iniated")

        for key, value in kwargs.items():
            # key = key.upper()
            parsed_value = self.parse(value)

            if key in self.values:
                original_value = self.__getattribute__(key)
                if isinstance(parsed_value, Settings) and isinstance(
                    original_value, Settings
                ):
                    object.__setattr__(self, key, original_value + parsed_value)
                else:
                    object.__setattr__(self, key, parsed_value)
            else:
                object.__setattr__(self, key, parsed_value)
                self.values.append(key)

    def set_values_from_yaml(self, path):
        """Set config from yaml file

        # Args
            path <str>: the path to config file
        """
        if self._initialized:
            raise AttributeError("Setting has been initiated, cannot be re-iniated")

        with open(path, "r") as f_in:
            config = yaml.safe_load(f_in)

            # set setting for other configs
            self.set_values(**config)

    def set_global_settings(self, dir_base=None, **kwargs):
        """Set global settings"""
        if self._initialized:
            raise AttributeError("Setting has been initiated, cannot be re-initiated")
        dir_base = get_base_dir() if dir_base is None else dir_base

        # set the system-level settings
        system_config = Path("/etc", c.FILE_CONFIG[1:])
        if system_config.exists():
            self.set_values_from_yaml(system_config)

        # set the user-level settings
        user_config = Path.home() / c.FILE_CONFIG
        if user_config.exists():
            self.set_values_from_yaml(user_config)

        # set the project-shared-remote-level settings
        project_config = Path(dir_base, c.FILE_CONFIG)
        if project_config.exists():
            self.set_values_from_yaml(project_config)

        # set the project-local-level settings
        project_config = Path(dir_base, c.FILE_LOCAL_CONFIG)
        if project_config.exists():
            self.set_values_from_yaml(project_config)

        # set the params settings
        for key, value in kwargs.items():
            if key in self._PARAM_ALLOWED_SETTINGS:
                self.set_values(**{key: value})

        # set directory configs
        for each_var in dir(c):
            constants = {"DIR_BASE": dir_base, "DIR_CWD": str(Path.cwd())}
            if each_var.isupper():
                constants[each_var] = str(Path(dir_base, getattr(c, each_var)))
            self.set_values(**constants)

        object.__setattr__(self, "_initialized", True)
        object.__setattr__(self, "values", tuple(self.values))

    def items(self):
        """Iterate key and value config"""
        for each_item in self.values:
            yield each_item, getattr(self, each_item)

    def as_list(self, list_):
        result = []
        for each_item in list_:
            if isinstance(each_item, Settings):
                result.append(each_item.as_dict())
            elif isinstance(each_item, (list, tuple)):
                result.append(self.as_list(each_item))
            else:
                result.append(each_item)
        return result

    def as_dict(self):
        """Return a dictionary representation of the setting"""
        result = {}
        for each_item in self.values:
            value = self.__getattribute__(each_item)
            if isinstance(value, Settings):
                value = value.as_dict()
            elif isinstance(value, (list, tuple)):
                value = self.as_list(value)
            result[each_item] = value
        return result

    def get(self, key, default):
        """Get config value"""
        if key not in self.values:
            return default
        return getattr(self, key)

    def __getitem__(self, key):
        """Allow accessing config value through string"""
        if key not in self.values:
            raise IndexError(f"{key} does not exist")
        return getattr(self, key)

    def __len__(self):
        """Get the amount of configs"""
        return len(self.values)

    def __str__(self):
        """Pretty string representation"""
        str_repr = []
        for each_item in self.values:
            value = self.__getattribute__(each_item)
            if isinstance(value, Settings):
                str_repr.append("  " * self._level + f"{each_item}:")
                str_repr.append(str(value))
            elif isinstance(value, (list, tuple)):
                str_repr.append("  " * self._level + f"{each_item}:")
                for each_config in value:
                    str_repr.append("  " * (self._level + 1) + f"- {each_config}")
            else:
                str_repr.append("  " * self._level + f"{each_item}: {value}")

        return "\n".join(str_repr)

    def __repr__(self):
        """Pretty string representation"""
        return self.__str__()

    def __add__(self, other):
        """Perform addition"""
        result = {}
        for each_value in self.values:
            if each_value in other.values:
                here = getattr(self, each_value)
                there = getattr(other, each_value)
                if isinstance(here, Settings) and isinstance(there, Settings):
                    result[each_value] = here + there
                else:
                    result[each_value] = there
            else:
                result[each_value] = getattr(self, each_value)

        for each_value in list(set(other.values).difference(self.values)):
            result[each_value] = getattr(other, each_value)

        settings = Settings(level=self._level)
        settings.set_values(**result)

        return settings


settings = Settings()
