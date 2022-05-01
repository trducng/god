"""Default constants used inside gods"""
from pathlib import Path

DIR_GOD = ".god"

DIR_INDICES = str(Path(DIR_GOD, "indices"))

DIR_HIDDEN_WORKING = str(Path(DIR_GOD, "workings"))
DIR_HIDDEN_WORKING_PLUGINS = str(Path(DIR_HIDDEN_WORKING, "plugins"))

DIR_CACHE = str(Path(DIR_GOD, "cache"))

DIR_REFS = str(Path(DIR_GOD, "refs"))
DIR_REFS_HEADS = str(Path(DIR_REFS, "heads"))

FILE_HEAD = str(Path(DIR_GOD, "HEAD"))
FILE_INDEX = str(Path(DIR_GOD, "index"))
FILE_CONFIG = "godconfig"
FILE_LOCAL_CONFIG = str(Path(DIR_GOD, FILE_CONFIG))
FILE_LINK = str(Path(DIR_GOD, "links"))
