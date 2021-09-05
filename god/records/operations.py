import re
import shutil
from collections import defaultdict
from pathlib import Path

from god.core.index import Index
from god.records.configs import get_records_config, RecordsConfig
from god.records.storage import get_internal_nodes, get_leaf_nodes
from god.utils.constants import RECORDS_INTERNALS, RECORDS_LEAVES
from god.utils.exceptions import RecordParsingError, RecordNotExisted


def copy_tree(root: str, dir_cache: str, dir_records: str) -> None:
    """Copy the tree from cache directory to records directory

    Args:
        root: the address of tree root
        dir_cache: directory storing working records
        dir_records: directory storing to-be-commited records
    """
    internal_nodes = get_internal_nodes(root, Path(dir_cache, RECORDS_INTERNALS))
    leaf_nodes = [
        each[0] for each in get_leaf_nodes(root, Path(dir_cache, RECORDS_INTERNALS))
    ]

    dir_cache = Path(dir_cache).resolve()
    dir_cache_internals = Path(dir_cache, RECORDS_INTERNALS)
    dir_cache_leaves = Path(dir_cache, RECORDS_LEAVES)

    dir_records = Path(dir_records).resolve()
    dir_records_internals = Path(dir_records, RECORDS_INTERNALS)
    dir_records_leaves = Path(dir_records, RECORDS_LEAVES)

    # copy internal nodes
    for node_hash in internal_nodes:
        source = dir_cache_internals / node_hash
        target = dir_records_internals / node_hash
        if target.is_file():
            continue
        shutil.copy(source, target)
        target.chmod(0o440)

    # copy leaf nodes
    for node_hash in leaf_nodes:
        source = dir_cache_leaves / node_hash
        target = dir_records_leaves / node_hash
        if target.is_file():
            continue
        shutil.copy(source, target)
        target.chmod(0o440)


def parse(files: list, config: RecordsConfig) -> dict:
    """Parse files into id and column.

    Args:
        files: list of files, relative to repo directory
        config: the records config

    Returns:
        Record information {"id": {"col": "val"}}
    """
    pattern = re.compile(config.get_pattern())
    conversion_groups = config.get_group_rule()

    result_dict = defaultdict(dict)  # {id: {col: val}}
    for fn in files:
        match = pattern.match(fn)
        if match is None:
            continue

        match_dict = match.groupdict()

        # get the id
        if "id" not in match_dict:
            continue

        id_ = match_dict.pop("id")
        for group, match_key in match_dict.items():
            if group in conversion_groups:
                match_value = conversion_groups[group][match_key]
                result_dict[id_][match_value] = fn
            else:
                result_dict[id_][group] = match_key

    return result_dict


def parse_strict(files: list, config: RecordsConfig) -> dict:
    """Strict version of Parsing files into id and column

    This function has similar input and output as `parse`. However, it additionally
    checks and raises inconsistencies during parsing. Inconsistencies include:
        - 2 different files match into the same id and column

    Args:
        files: list of files, relative to repo directory
        config: the records config

    Returns:
        Record information {"id": {"col": "val"}}
    """
    pattern = re.compile(config.get_pattern())
    conversion_groups = config.get_group_rule()
    path_cols = set(config.get_path_columns())

    result_dict = defaultdict(dict)  # {id: {col: [vals]}}
    for fn in files:
        match = pattern.match(fn)
        if match is None:
            continue

        match_dict = match.groupdict()

        # get the id
        if "id" not in match_dict:
            continue

        id_ = match_dict.pop("id")
        for group, match_key in match_dict.items():
            if group in conversion_groups:
                match_value = conversion_groups[group][match_key]
                items = result_dict[id_].get(match_value, [])
                items.append(fn)
                result_dict[id_][match_value] = items
            else:
                items = result_dict[id_].get(group, [])
                items.append(match_key)
                result_dict[id_][group] = items

    problems = []
    result = {}
    for id_, cols in result_dict.items():
        instance_result = {}
        for col_name, col_values in cols.items():
            if len(col_values) > 1:
                problems.append((id_, col_name, col_values))
            instance_result[col_name] = col_values[-1]
        result[id_] = instance_result

    if problems:
        for each in problems:
            print(each)
        raise RecordParsingError("Problems when parsing from files to records")

    return result


def path_to_record_id(files: list, config: RecordsConfig) -> dict:
    """Map from path to id

    Map from file path to records id.

    Args:
        files: list of files, relative to repo directory
        config: the records config

    Returns:
        Mapping {file-path: record-id}
    """
    pattern = re.compile(config.get_pattern())

    result = {}
    for fn in files:
        match = pattern.match(fn)
        if match is None:
            continue
        match_dict = match.groupdict()
        result[fn] = match_dict.pop("id", None)

    return result


def records_consistency(
    config1: RecordsConfig, config2: RecordsConfig, files: list
) -> dict:
    """Check for record consistency when applied on same set of files

    Args:
        config1: record config 1
        config2: record config 2
        files: list of file paths (relative to base dir)

    Returns:
        a set of files that have inconsistency {"file-path": ["id-conf1", "id-conf2"]}
    """
    problems = {}

    paths_ids1 = path_to_id(files, config1)
    paths_ids2 = path_to_id(files, config2)

    for fp in paths_ids1.keys():
        if paths_ids1[fp] != paths_ids2[fp]:
            problems[fp] = (paths_ids1[fp], paths_ids2[fp])

    return problems


def validate_records(config: RecordsConfig, records: dict, files: list) -> tuple:
    """Validate that records are consistent with config

    This operation checks for following conditions:
        - All non-auto columns in records are defined in config
        - There are ids in records that cannot be deduced from given list of files

    Args:
        config: the record config
        records: records that have following items
        files: files to treat

    Returns:
        [(str, str)]: list of ids that have unknown columns
        [str]: list of unknown ids
    """
    valid_cols = set(config.get_nonauto_columns())

    invalid_cols = []
    for id_, cols in records.items():
        for col in cols.keys():
            if col not in valid_cols:
                invalid_cols.append((id_, col))

    parsed_paths = parse(files, config)
    unknown_ids = set(records.keys()).difference(set(parsed_paths.keys()))
    unknown_ids = sorted(list(unknown_ids))

    return invalid_cols, unkown_ids


def check_records_conflict(index_path: str, dir_obj: str) -> bool:
    """Check for consistency between old configuration and new configuration

    Inconsistency happens when:
        - Results after parsing files from old config and new config do not agree
        for path -> ids

    Args:
        index_path: path to index file
        dir_obj: the path to object

    Returns:
        True if there is conflict, False otherwise
    """
    with Index(index_path) as index:
        godconfig = index.get_files_info(files=".godconfig")

    if not godconfig:  # no godconfig file
        return False

    godconfig = godconfig[0]
    if not godconfig[2]:  # no change in godconfig file
        return False

    # check new config file is valid
    new_config_file = godconfig[2]
    new_config: dict = get_records_config(
        Path(
            dir_obj,
            f"{new_config_file[:2]}",
            f"{new_config_file[2:4]}",
            f"{new_config_file[4:]}",
        )
    )

    for records_name, records_config in new_config.items():
        with Index(index_path) as index:
            record_index = index.get_records(name=records_name)
        if not record_index:
            raise RecordNotExisted(
                f'Cannot find record "{records_name}". '
                f"Please run `god records init {records_name}"
            )

    # check consistency between old and new config file
    old_config_file = godconfig[1]
    if not old_config_file:
        return True

    old_config: dict = get_records_config(
        Path(
            dir_obj,
            f"{old_config_file[:2]}",
            f"{old_config_file[2:4]}",
            f"{old_config_file[4:]}",
        )
    )

    return True


def init(name: str, index_path: str, dir_cache_records: str):
    """Initiate the records

    Args:
        name: the record name to initiate
        index_path: path to index file
    """
    # construct empty records

    # add entries to index
    pass
