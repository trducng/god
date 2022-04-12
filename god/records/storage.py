"""Implementation of the prolly tree data storage component

Design decision:
    - Efficient in detecting changes
    - Efficient for read, write, update, delete when amount of records increases
    - Can be used for other DBMS system (SQL or NoSQL)
    - Balance structural sharing with number of read/writes

Each internal node has the following information:
    - The hash value (calculated from child hash)
    - The number of ranges
    - The cut off value for each range
    - The hash value of each child

Each child node contains:
    - The hash value (calculated from the content)
    - All keys and values fall within this node. Each contains:
        + Key (str), value (json), value hash (str)
"""
import json
from collections import defaultdict
from hashlib import sha256
from pathlib import Path
from typing import Dict, List

from god.records.exceptions import RecordEntryNotFound
from god.records.utils import binary_search, one_line_sorted_json


def construct_leaf_node(records: dict, node_dir: str) -> str:
    """Construct the node

    Args:
        records: has format `{KEY: {col1: val1, col2: val2}}`
        node_dir: the directory to store the node

    Returns:
        the hash of node
    """
    content = json.dumps(records, sort_keys=True)
    hash_value = sha256(content.encode()).hexdigest()
    with Path(node_dir, hash_value).open("w") as fo:
        fo.write(content)

    return hash_value


def construct_internal_node(node_contents: List, node_dir: str) -> str:
    """Construct the node

    Args:
        node_contents: each item is a child node information (or a record if this is
            to construct leaf node)
        node_dir: the directory to store the node

    Returns:
        the hash of node
    """
    content = one_line_sorted_json(node_contents)
    hash_value = sha256(content.encode()).hexdigest()
    with Path(node_dir, hash_value).open("w") as fo:
        fo.write(content)

    return hash_value


def get_matching_child(
    key: str, child_nodes: List, start: int = None, end: int = None
) -> str:
    """Locate child node that contain keys

    This algorithm compare between `key` and `child_nodes[][2]` (stop boundary)

    Args:
        key: the key to search
        child_nodes: each item contains hash, start boundary and stop boundary
        start: the start index to search in `child_nodes`
        end: the end index to search in `child_nodes`

    Returns:
        the hash of child node
    """
    start = 0 if start is None else start
    end = len(child_nodes) - 1 if end is None else end

    if child_nodes[start][2] >= key:
        return child_nodes[start][0]

    if child_nodes[end][2] <= key:
        return child_nodes[end][0]

    if start + 1 == end:
        return child_nodes[end][0]

    middle = (start + end) // 2
    if child_nodes[middle][2] > key:
        return get_matching_child(key, child_nodes, start, middle)
    else:
        return get_matching_child(key, child_nodes, middle, end)


def get_keys_indices(keys: str, records: List) -> dict:
    """Get the position of `keys` in `records`

    Args:
        keys: the key to search, where each key is a string
        records: each item contains key and values

    Returns:
        the value of key or None if nothing match
    """
    record_keys = [list(each.keys())[0] for each in records]
    result = {}
    for key in keys:
        key_idx = binary_search(key, record_keys)
        result[key] = key_idx

    return result


def get_keys_values(keys: str, records: List) -> dict:
    """Search content in the leaf node

    Args:
        keys: the key to search
        records: each item contains key and values

    Returns:
        the value of key or None if nothing match
    """
    record_keys = [list(each.keys())[0] for each in records]
    result = {}
    for key in keys:
        key_idx = binary_search(key, record_keys)
        result[key] = records[key_idx][key] if key_idx is not None else None

    return result


def get_leaf_nodes(root: str, tree_dir: str, sort_keys: bool = False) -> List:
    """Get all leaf nodes that has `root` as parent

    Args:
        root: the hash of root node
        tree_dir: the directory storing root node and intermediate nodes
        sort_keys: if True, sort the leaf nodes by end keys

    Returns:
        list of (leaf node hash, start key, end key), sorted by end key in increasing
            order
    """
    if not Path(tree_dir, root).exists():
        return []

    with Path(tree_dir, root).open("r") as f_in:
        child_nodes = json.load(f_in)

    result = []
    for child_hash, start_key, end_key in child_nodes:
        temp = get_leaf_nodes(child_hash, tree_dir)
        if temp:
            result += temp
        else:
            result.append([child_hash, start_key, end_key])

    if sort_keys:
        result = sorted(result, key=lambda obj: obj[2])

    return result


def get_internal_nodes(root: str, tree_dir: str, sort_keys: bool = False) -> List:
    """Get all internal nodes that have `root` as parent

    Args:
        root: the hash of root node
        tree_dir: the directory storing root node and intermediate nodes
        sort_keys: if True, sort the leaf nodes by end keys

    Returns:
        List of node hash
    """
    with Path(tree_dir, root).open("r") as f_in:
        child_nodes = json.load(f_in)

    result = [root]
    for child_hash, start_key, end_key in child_nodes:
        if not Path(tree_dir, child_hash).exists():
            return result
        result += get_internal_nodes(child_hash, tree_dir)

    if sort_keys:
        result = sorted(result)

    return result


def get_records(root: str, tree_dir: str, leaf_dir: str) -> dict:
    """Get records from `root`

    Args:
        root: the hash of root node
        tree_dir: the directory storing root node and intermediate nodes
        leaf_dir: the directory containing leaf nodes

    Returns:
        All records with format {"id": {"col": "val"}}
    """
    leaf_nodes = get_leaf_nodes(root, tree_dir, sort_keys=True)

    result = {}
    for leaf_hash, _, _ in leaf_nodes:
        with Path(leaf_dir, leaf_hash).open("r") as fi:
            result.update(json.load(fi))

    return result


def build_tree_trunk(nodes: List, window: int, tree_dir: str) -> str:
    """Build intermediate nodes

    Args:
        nodes: list of intermediate nodes
        window: the size of windows to take care of. The smaller the window, the more
            likely the path will resemble between items
        tree_dir: the output directory to store the tree

    Returns:
        hash of root node
    """
    if not nodes:
        raise AttributeError("empty nodes")

    if len(nodes) == 1 and Path(tree_dir, nodes[0][0]).is_file():
        return nodes[0][0]

    start_key_hashes = [
        sha256(start_key.encode()).hexdigest() for _, start_key, _ in nodes
    ]
    new_nodes = []
    last_idx = 0
    for idx, (node_hash, start_key, end_key) in enumerate(nodes):
        if idx + 1 < last_idx + window:
            continue

        window_hash = sha256(
            "".join(start_key_hashes[idx - window + 1 : idx + 1]).encode()
        ).hexdigest()
        if window_hash[:3] == "000":
            hash_value = construct_internal_node(nodes[last_idx : idx + 1], tree_dir)
            new_nodes.append((hash_value, nodes[last_idx][1], end_key))
            last_idx = idx + 1

    if last_idx != len(nodes):
        hash_value = construct_internal_node(nodes[last_idx:], tree_dir)
        new_nodes.append((hash_value, nodes[last_idx][1], nodes[-1][2]))

    return build_tree_trunk(nodes=new_nodes, window=window, tree_dir=tree_dir)


def prolly_create(records: dict, tree_dir: str, leaf_dir: str) -> str:
    """Create a prolly tree containing the items

    Instead of using a rolling hash, we can use the hash of the key, as by our usage,
    the key is the primary key used to index the information.

    Args:
        records: has format `{KEY: {col1: val1, col2: val2}}`
        tree_dir: the output directory to store the tree
        leaf_dir: the directory containing leaf nodes

    Returns:
        hash value of root node
    """
    if not records:
        # construct empty tree
        return construct_internal_node([], tree_dir)

    items = sorted(list(records.keys()))

    # handle leaf nodes
    last_idx = 0
    nodes = []
    for idx, each_key in enumerate(items):
        key_hash = sha256(each_key.encode()).hexdigest()
        if key_hash[:3] == "000":
            to_save = {key: records[key] for key in items[last_idx : idx + 1]}
            hash_value = construct_leaf_node(to_save, leaf_dir)
            nodes.append((hash_value, items[last_idx], each_key))
            last_idx = idx + 1

    if last_idx != len(items):
        # resolve dangling entries
        to_save = {key: records[key] for key in items[last_idx:]}
        hash_value = construct_leaf_node(to_save, leaf_dir)
        nodes.append((hash_value, items[last_idx], items[-1]))

    return build_tree_trunk(nodes, 2, tree_dir)


def get_paths_to_records(keys: List, root: str, tree_dir: str) -> dict:
    """Search for result inside a prolly tree

    Args:
        keys: each item is a string, denoting a key to search
        root: the address of tree root
        tree_dir: the output directory to store the tree

    Returns:
        {key: [non-leaf hashes]} where key is the key and non-leaf hashes are from
            top-most (root) to bottom-most non-leaf nodes
    """
    # locate into inner-most non-leaf node
    result = defaultdict(list)
    buff = defaultdict(list)
    for key in keys:
        result[key].append(root)

    if not Path(tree_dir, root).exists():
        # This is not non-leaf tree, skip
        return result

    with Path(tree_dir, root).open("r") as f_in:
        child_nodes = json.load(f_in)
        if not child_nodes:
            return {key: [None] for key in keys}

    for key in keys:
        child_hash = get_matching_child(key, child_nodes)
        buff[child_hash].append(key)

    for child_hash, child_keys in buff.items():
        sub_result = get_paths_to_records(child_keys, child_hash, tree_dir)
        for child_key, nodes in sub_result.items():
            result[child_key] += nodes

    return result


def prolly_locate(keys: List, root: str, tree_dir: str, leaf_dir: str) -> dict:
    """Search for result inside a prolly tree

    Args:
        keys: each item is a string, denoting a key to search
        root: the address of tree root
        tree_dir: the output directory to store the tree
        leaf_dir: the directory containing leaf nodes

    Returns:
        Dict of items that match, each item is a dict that has following structure
            {key: {col1: val1, col2: val2}} or {key: None} if key does not exist
    """
    prolly_paths = get_paths_to_records(keys, root, tree_dir)

    buff = defaultdict(list)
    for key, leaf_hash in prolly_paths.items():
        buff[leaf_hash[-1]].append(key)

    result = {key: None for key in buff.pop(None, [])}
    for leaf_hash, buff_keys in buff.items():
        with Path(leaf_dir, leaf_hash).open("r") as f_in:
            leaf_contents = json.load(f_in)
        for buff_key in buff_keys:
            result[buff_key] = (
                leaf_contents[buff_key] if buff_key in leaf_contents else None
            )

    return result


def adjust_intermediate_nodes(
    paths: List, tree_dir: str, resolved: dict = None
) -> List:
    """Adjust intermediate nodes to new hash values, assuming no bucketting changes

    Args:
        paths: list of list of nodes, each inner list is a path from high level to
            low level. Each item in the inner list is a node has value
        tree_dir: the output directory to store the tree
        resolved: buffering to store, can has any of these formats:
            - update: {old_hash_value: new_hash_value}

    Returns:
        list of list of new node hash values
    """
    resolved = {} if resolved is None else resolved
    buff = defaultdict(list)
    max_level = len(max(paths, key=lambda obj: len(obj)))

    for level in range(max_level - 1, -1, -1):
        # iterate from lower level to higher level
        for idx, path in enumerate(paths):
            old_hash = path[level]
            if old_hash in resolved:
                buff[idx].append(resolved[old_hash])
                continue
            with Path(tree_dir, old_hash).open("r") as fi:
                childrens = json.load(fi)

            new_children = []
            for child in childrens:
                if child[0] in resolved:
                    new_children.append((resolved[child[0]], *child[1:]))
                else:
                    new_children.append(child)

            new_hash = construct_internal_node(new_children, tree_dir)
            resolved[old_hash] = new_hash
            buff[idx].append(new_hash)

    result = []
    for idx in range(len(buff)):
        result.append(list(reversed(buff[idx])))

    return result


def prolly_update(records: dict, root: str, tree_dir: str, leaf_dir: str) -> str:
    """Update the tree.

    This method assumes that each value in `records` relates involves updating column
    value, not adding or deleting columns.

    Args:
        records: has format `{KEY: {col1: val1, col2: val2}}`
        root: the address of tree root
        tree_dir: the output directory to store the tree
        leaf_dir: the directory containing leaf nodes

    Returns:
        the new root node hash value
    """
    keys = list(records.keys())

    prolly_paths = get_paths_to_records(keys, root, tree_dir)
    buff = defaultdict(list)
    for key, leaf_hash in prolly_paths.items():
        buff[leaf_hash[-1]].append(key)

    resolved = {}  # {old_hash: new_hash}
    for leaf_hash, temp_keys in buff.items():
        with Path(leaf_dir, leaf_hash).open("r") as fi:
            leaf_nodes = json.load(fi)
        for temp_key in temp_keys:
            if temp_key not in leaf_nodes:
                raise RecordEntryNotFound(f"{temp_key} not found in {leaf_hash}")
            leaf_nodes[temp_key].update(records[temp_key])

        hash_value = construct_leaf_node(leaf_nodes, leaf_dir)
        resolved[leaf_hash] = hash_value

    # update to the root node
    nodes = adjust_intermediate_nodes(list(prolly_paths.values()), tree_dir, resolved)
    if nodes:
        return nodes[0][0]

    return ""


def prolly_insert(records: List, root: str, tree_dir: str, leaf_dir: str) -> str:
    """Insert items into the tree

    Inserting items will be quick if it just inserts at the middle of the bucket. If
    the insertion happens at the beginning or ending of the bucket, we will need to
    re-calibrate the whole tree root. We should aim for the worst first, then aim
    for the easiest case (the easiest case can be quickly resolved).

    Worst case scenario - inserting at the end/beginning of bucket:
        1. get all buckets
        2. insertion inside the buckets
        3. deletion inside the buckets
        4. role out all items again
        5. perform tree construction

    Args:
        records: has format `{KEY: {col1: val1, col2: val2}}`
        root: the address of tree root
        tree_dir: the output directory to store the tree
        leaf_dir: the directory containing leaf nodes

    Returns:
        the new root node hash value
    """
    all_records = get_records(root, tree_dir, leaf_dir)
    all_records.update(records)

    return prolly_create(all_records, tree_dir, leaf_dir)


def prolly_delete(keys: List, root: str, tree_dir: str, leaf_dir: str) -> str:
    """Delete records from the tree that match `keys`

    Args:
        keys: each item is a string containing key values
        root: the address of tree root
        tree_dir: the output directory to store the tree
        leaf_dir: the directory containing leaf nodes

    Returns:
        the hash of new root tree
    """
    records = get_records(root, tree_dir, leaf_dir)
    for key in keys:
        del records[key]

    return prolly_create(records, tree_dir, leaf_dir)


def prolly_edit(
    root: str,
    tree_dir: str,
    leaf_dir: str,
    insert: Dict = None,
    update: Dict = None,
    delete: List = None,
) -> str:
    """Edit prolly tree with insert, update and delete

    Args:
        root: the address of tree root
        tree_dir: the output directory to store the tree
        leaf_dir: the directory containing leaf nodes
        insert: keys and values to insert
        update: keys and values to update
        delete: keys to delete

    Returns:
        The new root hash
    """
    delete_temp = []
    delete = delete or []
    insert = insert or {}

    # update records
    update_records = {}
    if update:
        update_records = prolly_locate(list(update.keys()), root, tree_dir, leaf_dir)
        for key, record in update_records.items():
            record.update(update[key])
            cols = record.keys()
            for col in cols:
                if record[col] is None:
                    record.pop(col)

        delete_temp += list(update.keys())

    # delete records
    delete_ = delete + delete_temp
    records = get_records(root, tree_dir, leaf_dir)
    for record in delete_:
        del records[record]

    # insert records
    records.update(insert)
    records.update(update_records)

    return prolly_create(records, tree_dir, leaf_dir)
