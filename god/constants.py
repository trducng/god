"""Default constants used inside gods"""
from pathlib import Path

DIR_GOD = ".god"

DIR_COMMITS = str(Path(DIR_GOD, "commits"))
DIR_COMMITS_DIRECTORY = str(Path(DIR_COMMITS, "dirs"))

DIR_RECORDS = str(Path(DIR_GOD, "records"))
DIR_RECORDS_LOG = str(Path(DIR_RECORDS, "logs"))
DIR_RECORDS_DB = str(Path(DIR_RECORDS, "db"))
DIR_RECORDS_CACHE = str(Path(DIR_RECORDS, "cache"))

DIR_REFS = str(Path(DIR_GOD, "refs"))
DIR_REFS_HEADS = str(Path(DIR_REFS, "heads"))

DIR_SNAPS = str(Path(DIR_GOD, "snapshots"))

DEFAULT_DIR_OBJECTS = str(Path(DIR_GOD, "objects"))

FILE_HEAD = str(Path(DIR_GOD, "HEAD"))
FILE_INDEX = str(Path(DIR_GOD, "index"))
FILE_RECORD = ".godrecord"
FILE_CONFIG = ".godconfig"
FILE_LOCAL_CONFIG = str(Path(DIR_GOD, FILE_CONFIG))
