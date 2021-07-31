import json
from pathlib import Path

from god.utils.common import binary_search
from god.utils.exceptions import InternalNodeNotFound


def read_leaf(leaf, leaf_dir):
    """Read leaf content into dictionary. Temporary."""
    with Path(leaf_dir, leaf).open("r") as fi:
        content = json.load(fi)

    content = {list(each.keys())[0]: list(each.values())[0] for each in content}
    return content


def get_root(root, node_dir):
    with Path(node_dir, root).open("r") as fi:
        data = json.load(fi)

    return [root, data[0][1], data[-1][2]]


def get_leftmost_leaf(root: str, node_dir: str, cache: dict = None) -> list:
    """Get the left-most leaf from the root

    Args:
        root: the hash of the internal node
        node_dir: the directory containing internal node
        cache: the cache dictionary

    Returns:
        List of items (str, str, str): the hash, start, end key of all nodes until
            leaf node.
    """
    if not Path(node_dir, root).exists():
        # not in internal node dir means `root` is a leaf node
        raise InternalNodeNotFound(f"Node {root} is not internal node")

    if cache is not None and root in cache:
        content = cache[root]
    else:
        with Path(node_dir, root).open("r") as fi:
            content = json.load(fi)

        if cache is not None:
            cache[root] = content

    if Path(node_dir, content[0][0]).exists():
        return [content[0]] + get_leftmost_leaf(content[0][0], node_dir, cache)

    return [content[0]]


def get_next_sibling(key: str, parent: str, node_dir: str, cache: dict = None) -> list:
    """Get the next sibling from current child node that contains key `key`

    Args:
        key: the hash value of current child node
        parent: the internal node that is direct parent of current leaf
        node_dir: the directory containing internal node
        cache: the cache dictionary to avoid reading from disk too many times

    Returns:
        the hash value of next sibling or None if this is the last sibling
    """
    if isinstance(cache, dict) and parent in cache:
        content = cache[parent]
    else:
        with Path(node_dir, parent).open("r") as fi:
            content = json.load(fi)

    if isinstance(cache, dict):
        cache[parent] = content

    node_idx = binary_search(key, [_[2] for _ in content])
    return content[node_idx + 1]


def get_next_leaf(nodes: list, node_dir: str, cache: dict = None) -> list:
    """Get the next leaf from current leaf

    Args:
        nodes: the list of internal nodes from top to bottom, containing leaf node.
            Each node contains (node hash, start key, end key). The last node in
            `nodes` is expected to be leaf node
        node_dir: the directory containing internal node
        cache: the cache dictionary to avoid reading from disk too many times

    Returns:
        the full graph to next leaf node, each item is a node, containing node hash,
            start key, end key
    """
    bottom_up = list(reversed(nodes))[1:]  # excluding the leaf node
    leaf_hash, leaf_start, leaf_end = nodes[-1]

    travel_next_node = False
    for idx, (parent_hash, parent_start, parent_end) in enumerate(bottom_up):
        if leaf_end == parent_end:
            # if travel_next_node is True, we have to travel to the next internal node
            # sibling, else, we just pick the next item
            travel_next_node = True
            continue

        if isinstance(cache, dict) and parent_hash in cache:
            content = cache[parent_hash]
        else:
            with Path(node_dir, parent_hash).open("r") as fi:
                content = json.load(fi)

        if isinstance(cache, dict):
            cache[parent_hash] = content

        if travel_next_node:
            next_internal_node = get_next_sibling(
                leaf_end, parent_hash, node_dir, cache
            )
            child_paths = get_leftmost_leaf(next_internal_node[0], node_dir, cache)
            return nodes[: -(idx + 1)] + [next_internal_node] + child_paths
        else:
            leaf_idx = binary_search(leaf_end, [_[2] for _ in cache[parent_hash]])
            return nodes[:-1] + [content[leaf_idx + 1]]

    return None  # no match, must be end of tree


def transform_dict(dict1: dict, dict2: dict) -> list:
    """Transform from dict1 to dict2

    Args:
        dict1: dictionarr1
        dict2: dictionary2

    Returns:
        {} the add columns {col_name: col_value}
        {} the updated columns {col_name: [old_value, new_value]}
        {} the deleted columns {col_name: col_value}
    """
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())

    add = {key: dict2[key] for key in keys2.difference(keys1)}
    update = {
        key: [dict1[key], dict2[key]]
        for key in keys1.intersection(keys2)
        if dict1[key] != dict2[key]
    }
    delete = {key: dict1[key] for key in keys1.difference(keys2)}

    return add, update, delete


def compare_leaves(leaf1: str, leaf2: str, leaf_dir: str, cache: dict = None) -> list:
    """Compare 2 leaves. Create transformation from leaf1 -> leaf2

    This operation assumes `leaf1` comes from tree1 and `leaf2` comes from tree2.

    Args:
        leaf1: the hash value of leaf1
        leaf2: the hash value of leaf2
        leaf_dir: the directory containing leaf node
        cache: the cache dictionary to avoid reading from disk too many times

    Returns:
        {}: dictionary of add operations (not in leaf1, in leaf2)
        {}: dictionary of update operations (modify values from leaf1 -> leaf2)
        {}: dictionary of remove operations (in leaf1, not in leaf2)
    """
    if leaf1 == leaf2:
        return {}, {}, {}

    if leaf1 in cache:
        content1 = cache[leaf1]
    else:
        with Path(leaf_dir, leaf1).open("r") as fi:
            content1 = json.load(fi)

    if leaf2 in cache:
        content2 = cache[leaf2]
    else:
        with Path(leaf_dir, leaf2).open("r") as fi:
            content2 = json.load(fi)

    content1 = {list(each.keys())[0]: list(each.values())[0] for each in content1}
    content2 = {list(each.keys())[0]: list(each.values())[0] for each in content2}
    content1_keys = set(content1.keys())
    content2_keys = set(content2.keys())

    add = {key: content2[key] for key in content2_keys.difference(content1_keys)}
    update = {
        key: transform_dict(content1[key], content2[key])
        for key in content1_keys.intersection_update(content2.keys())
    }
    remove = {key: content1[key] for key in content1_keys.difference(content2_keys)}

    return add, update, remove


def compare_tree(tree1: str, tree2: str, node_dir: str, leaf_dir: str) -> list:
    """Compare 2 trees. Get the transformation from tree1 to tree2

    Args:
        tree1: the hash value of root node of tree1
        tree2: the hash value of root node of tree2
        node_dir: the directory containing internal node
        leaf_dir: the directory containing leaf node

    Returns:
        {}: dictionary of add operations (not in tree1, in tree2)
        {}: dictionary of update operations (modify values from tree1 -> tree2)
        {}: dictionary of remove operations (in tree1, not in tree2)
    """
    cache = {}
    add, update, remove = {}, {}, {}

    # get left-most leaf and populate cache
    paths1 = [get_root(tree1)] + get_leftmost_leaf(tree1, node_dir, cache)
    paths2 = [get_root(tree2)] + get_leftmost_leaf(tree2, node_dir, cache)

    while paths1 is not None or paths2 is not None:

        if paths1 is None:
            leaf2, start2, end2 = paths2[-1]
            add.update(read_leaf(leaf2, leaf_dir))
            paths2 = get_next_leaf(paths2, node_dir, cache)
            del cache[leaf2]
            continue
        if paths2 is None:
            leaf1, start1, end1 = paths1[-1]
            remove.update(read_leaf(leaf1, leaf_dir))
            del cache[leaf1]
            continue

        leaf1, start1, end1 = paths1[-1]
        leaf2, start2, end2 = paths2[-1]

        if start1 > end2:
            # tree1 does not have leaf2
            add.update(read_leaf(leaf2, leaf_dir))
            paths2 = get_next_leaf(paths2, node_dir, cache)
            del cache[leaf2]
        elif start2 > end1:
            # tree2 does not have leaf1
            remove.update(read_leaf(leaf1, leaf_dir))
            paths1 = get_next_leaf(paths1, node_dir, cache)
            del cache[leaf1]
        elif start1 == start2 and end1 == end2:
            # two leaves match completely
            radd, rupdate, rremove = compare_leaves(leaf1, leaf2, leaf_dir, cache)
            add.update(radd)
            update.update(rupdate)
            remove.update(rremove)
            paths1 = get_next_leaf(paths1, node_dir, cache)
            paths2 = get_next_leaf(paths2, node_dir, cache)
            del cache[leaf1]
            del cache[leaf2]
        elif (start1 < start2 and end1 > end2) or (start1 > start2 and end1 > end2):
            # leaf2 inside leaf1, advance leaf2
            radd, rupdate, rremove = compare_leaves(leaf1, leaf2, leaf_dir, cache)
            add.update(radd)
            update.update(rupdate)
            remove.update(rremove)
            paths2 = get_next_leaf(paths2, node_dir, cache)
            del cache[leaf2]
        elif (start1 > start2 and end1 < end2) or (start1 < start2 and end1 < end2):
            # leaf1 inside leaf2, advance leaf1
            radd, rupdate, rremove = compare_leaves(leaf1, leaf2, leaf_dir, cache)
            add.update(radd)
            update.update(rupdate)
            remove.update(rremove)
            paths1 = get_next_leaf(paths1, node_dir, cache)
            del cache[leaf1]

    return add, update, remove
