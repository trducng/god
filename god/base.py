"""Base functions and constants. Helpful for other functions to build up."""
from dataclasses import dataclass
from pathlib import Path

import yaml


GOD_DIR = ".god"

OBJ_DIR = f"{GOD_DIR}/objects"
MAIN_DIR = f"{GOD_DIR}/main"

LOG_DIR = f"{MAIN_DIR}/logs"
DB_DIR = f"{MAIN_DIR}/db"
CACHE_DIR = f"{MAIN_DIR}/cache"
POINTER_FILE = f"{MAIN_DIR}/pointers"

CONFIG_FILE = ".godconfig.yml"


def get_base_dir(path=None):
    """Get `god` base dir from `path`

    # Args
        path <str>: the directory

    # Returns
        <str>: the directory that contains `.god` directory
    """
    if path is None:
        path = Path.cwd().resolve()

    current_path = Path(path).resolve()
    must_exist = [GOD_DIR, OBJ_DIR, MAIN_DIR]

    while True:
        fail = False
        for each_must in must_exist:
            if not (current_path / each_must).exists():
                fail = True
                break

        if fail:
            if current_path.parent == current_path:
                # this is root directory
                raise RuntimeError("Unitialized god repo. Please run `got init`")
            current_path = current_path.parent

        else:
            return str(current_path)


def change_index(value):
    """Change pointer information"""
    base_path = get_base_dir()
    pointer_file = Path(base_path, POINTER_FILE)

    with pointer_file.open("w") as f_out:
        f_out.write(value)


def get_db_dir(base_path=None, db_name=None):
    """Get DB directory"""
    if base_path is None:
        base_path = get_base_dir()

    base_path = Path(base_path).resolve()

    db_dir = base_path / DB_DIR
    if db_name is not None:
        db_dir /= db_name

    return str(db_dir)


def get_current_commit_db(base_path=None):
    """Get the current commit db

    # Args
        base_path <str>: the base directory that contains `.god`. If blank, infer
            from current working directory

    # Returns
        <str>: the name of current commit DB. If '', then there isn't a current
            commit database
    """
    base_path = get_base_dir(base_path)
    pointer_file = Path(base_path, POINTER_FILE)

    if not pointer_file.exists():
        return ""

    with pointer_file.open("r") as f_in:
        current_db = f_in.read().splitlines()[0]

    return current_db


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
            result = Settings(level=self._level+1)
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
            key = key.upper()
            parsed_value = self.parse(value)

            if key in self.values:
                original_value = self.__getattribute__(key)
                if (isinstance(parsed_value, Settings)
                        and isinstance(original_value, Settings)):
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

        with open(path, 'r') as f_in:
            config = yaml.safe_load(f_in)
            self.set_values(**config)

    def set_global_settings(self, **kwargs):
        """Set global settings"""
        if self._initialized:
            raise AttributeError("Setting has been initiated, cannot be re-initiated")

        # set the system-level settings
        system_config = Path('/etc', CONFIG_FILE[1:])
        if system_config.exists():
            self.set_values_from_yaml(system_config)

        # set the user-level settings
        user_config = Path.home() / CONFIG_FILE
        if user_config.exists():
            self.set_values_from_yaml(user_config)

        # set the project-level settings
        project_config = Path(get_base_dir(), CONFIG_FILE)
        if project_config.exists():
            self.set_values_from_yaml(project_config)

        # set the params settings
        for key, value in kwargs.items():
            if key.upper() in self._PARAM_ALLOWED_SETTINGS:
                self.set_values(**{key: value})

        object.__setattr__(self, "_initialized", True)
        object.__setattr__(self, "values", tuple(self.values))

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
                    str_repr.append("  " * (self._level+1) + f"- {each_config}")
            else:
                str_repr.append("  " * self._level + f"{each_item}: {value}")

        return '\n'.join(str_repr)

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
