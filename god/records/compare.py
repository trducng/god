import json
from pathlib import Path

from god.utils.common import binary_search


def get_root(root: str, node_dir: str) -> list:
    """Read the root and return [hash, start, end] format similar to other nodes

    Args:
        root: the hash of root node
        node_dir: the directory containing internal node

    Returns:
        hash, start key, end key
    """
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
        # raise InternalNodeNotFound(f"Node {root} is not internal node")
        return []

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


def get_rightmost_leaf(root: str, node_dir: str, cache: dict = None) -> list:
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
        # raise InternalNodeNotFound(f"Node {root} is not internal node")
        return []

    if cache is not None and root in cache:
        content = cache[root]
    else:
        with Path(node_dir, root).open("r") as fi:
            content = json.load(fi)

        if cache is not None:
            cache[root] = content

    if Path(node_dir, content[-1][0]).exists():
        return [content[-1]] + get_rightmost_leaf(content[-1][0], node_dir, cache)

    return [content[-1]]


def get_next_sibling(key: str, parent: str, node_dir: str, cache: dict = None) -> list:
    """Get the next sibling from current child node that contains key `key`

    Args:
        key: the end key of the current child node
        parent: the internal node that is direct parent of current leaf
        node_dir: the directory containing internal node
        cache: the cache dictionary to avoid reading from disk too many times

    Returns:
        the hash, start and end of next sibling or None if this is the last sibling
    """
    if isinstance(cache, dict) and parent in cache:
        content = cache[parent]
    else:
        with Path(node_dir, parent).open("r") as fi:
            content = json.load(fi)

    if isinstance(cache, dict):
        cache[parent] = content

    node_idx = binary_search(key, [_[2] for _ in content])
    if node_idx == len(content) - 1:
        return None
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
        or None if there is no add, update or delete
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
    if add or update or delete:
        return add, update, delete
    return None


def compare_leaves(leaves1: list, leaves2: list, leaf_dir: str) -> list:
    """Compare 2 set of leaves. Create transformation from leaves1 -> leaves2

    This operation assumes `leaf1` comes from tree1 and `leaf2` comes from tree2.

    Args:
        leaf1: the list of hash value of leaf1 in tree1
        leaf2: the list of hash  value of leaf2 in tree2
        leaf_dir: the directory containing leaf node

    Returns:
        {}: dictionary of add operations (not in leaf1, in leaf2)
        {}: dictionary of update operations (modify values from leaf1 -> leaf2)
        {}: dictionary of remove operations (in leaf1, not in leaf2)
    """
    content1, content2 = {}, {}
    for leaf1 in leaves1:
        with Path(leaf_dir, leaf1).open("r") as fi:
            content1.update(json.load(fi))

    for leaf2 in leaves2:
        with Path(leaf_dir, leaf2).open("r") as fi:
            content2.update(json.load(fi))

    content1_keys = set(content1.keys())
    content2_keys = set(content2.keys())

    add = {key: content2[key] for key in content2_keys.difference(content1_keys)}
    update = {}
    for key in content1_keys.intersection(content2.keys()):
        value = transform_dict(content1[key], content2[key])
        if value is not None:
            update[key] = value
    remove = {key: content1[key] for key in content1_keys.difference(content2_keys)}

    return add, update, remove


def skip_next_leaves(
    paths1: list, paths2: list, node_dir: str, cache: dict = None
) -> list:
    """Skip to the next leaves in case there are common paths

    Example:
        >> paths1 = ['a', 'b', 'c', 'd', 'e']
        >> paths2 = ['x', 'y', 'c', 'd', 'e']
        >> new1, new2 = skip_next_leaves(paths1, paths2, node_dir)
        >> print(new1)
        ['a', 'b', 'branch1', 'b11', 'b12']
        >> print(new2)
        ['x', 'y', 'branch2', 'b21', 'b22']

    This way, we can skip checking all the buckets from 'e' -> 'b12' for tree1, and
    'e' -> 'b22' for tree2.

    Args:
        paths1: full path to leaf1
        paths2: full path to leaf2
        node_dir: the directory containing node directory

    Returns:
        []: full path to next leaf1
        []: full path to next leaf2
    """
    if paths1[-1] != paths2[-1]:
        # if the last leaves are different, use normal slow method
        return (
            get_next_leaf(paths1, node_dir, cache),
            get_next_leaf(paths2, node_dir, cache),
        )

    leaf1, start1, end1 = paths1[-1]
    leaf2, start2, end2 = paths2[-1]
    if end1 == paths1[-2][-1] or end2 == paths2[-2][-1]:
        # make sure that the leaf is not at the end of any internal node
        return (
            get_next_leaf(paths1, node_dir, cache),
            get_next_leaf(paths2, node_dir, cache),
        )

    # get common roots
    bottom_up1 = list(reversed(paths1))[1:]
    bottom_up2 = list(reversed(paths2))[1:]

    level = 0
    for idx in range(min(len(bottom_up1), len(bottom_up2))):
        if bottom_up1[idx][0] != bottom_up2[idx][0]:
            level = idx
            break

    # this is where 2 trees even have common root node
    if len(bottom_up1) == len(bottom_up2) == level + 1:
        return None, None

    next_sibling1 = get_next_sibling(end1, bottom_up1[level][0], node_dir, cache)
    next_sibling2 = get_next_sibling(end2, bottom_up2[level][0], node_dir, cache)
    while True:
        if next_sibling1[0] == next_sibling2[0]:
            temp1 = get_next_sibling(
                next_sibling1[-1], bottom_up1[level][0], node_dir, cache
            )
            temp2 = get_next_sibling(
                next_sibling2[-1], bottom_up2[level][0], node_dir, cache
            )
            if temp1 is None or temp2 is None:
                return (
                    paths1[: -(level + 1)]
                    + [next_sibling1]
                    + get_rightmost_leaf(next_sibling1[0], node_dir, cache),
                    paths2[: -(level + 1)]
                    + [next_sibling2]
                    + get_rightmost_leaf(next_sibling2[0], node_dir, cache),
                )
            else:
                next_sibling1, next_sibling2 = temp1, temp2
        else:
            return (
                paths1[: -(level + 1)]
                + [next_sibling1]
                + get_leftmost_leaf(next_sibling1[0], node_dir, cache),
                paths2[: -(level + 1)]
                + [next_sibling2]
                + get_leftmost_leaf(next_sibling2[0], node_dir, cache),
            )

    # normal slow method if something unexpected
    return (
        get_next_leaf(paths1, node_dir, cache),
        get_next_leaf(paths2, node_dir, cache),
    )


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

    leaves1, leaves2 = [], []
    # get left-most leaf and populate cache
    paths1 = [get_root(tree1, node_dir)] + get_leftmost_leaf(tree1, node_dir, cache)
    paths2 = [get_root(tree2, node_dir)] + get_leftmost_leaf(tree2, node_dir, cache)

    while paths1 is not None or paths2 is not None:
        if paths1 is None:
            leaf2, start2, end2 = paths2[-1]
            with Path(leaf_dir, leaf2).open("r") as fi:
                add.update(json.load(fi))
            paths2 = get_next_leaf(paths2, node_dir, cache)
            continue
        if paths2 is None:
            leaf1, start1, end1 = paths1[-1]
            with Path(leaf_dir, leaf1).open("r") as fi:
                remove.update(json.load(fi))
            paths1 = get_next_leaf(paths1, node_dir, cache)
            continue

        leaf1, start1, end1 = paths1[-1]
        leaf2, start2, end2 = paths2[-1]

        if start1 > end2:
            # tree1 does not have leaf2
            with Path(leaf_dir, leaf2).open("r") as fi:
                add.update(json.load(fi))
            paths2 = get_next_leaf(paths2, node_dir, cache)
        elif start2 > end1:
            # tree2 does not have leaf1
            with Path(leaf_dir, leaf1).open("r") as fi:
                remove.update(json.load(fi))
            paths1 = get_next_leaf(paths1, node_dir, cache)
        elif end1 == end2:
            # two leaves match completely
            leaves1.append(leaf1)
            leaves2.append(leaf2)
            leaves1 = list(set(leaves1))
            leaves2 = list(set(leaves2))

            radd, rupdate, rremove = compare_leaves(leaves1, leaves2, leaf_dir)
            add.update(radd)
            update.update(rupdate)
            remove.update(rremove)
            paths1, paths2 = skip_next_leaves(paths1, paths2, node_dir, cache)
            leaves1, leaves2 = [], []
        elif end1 > end2:
            # leaf2 inside leaf1, advance leaf2
            leaves1.append(leaf1)
            leaves2.append(leaf2)
            paths2 = get_next_leaf(paths2, node_dir, cache)
        elif end1 < end2:
            # leaf1 inside leaf2, advance leaf1
            leaves1.append(leaf1)
            leaves2.append(leaf2)
            paths1 = get_next_leaf(paths1, node_dir, cache)

    # final check for leaves1 and leaves2
    if leaves1 or leaves2:
        radd, rupdate, rremove = compare_leaves(leaves1, leaves2, leaf_dir)
        add.update(radd)
        update.update(rupdate)
        remove.update(rremove)

    return add, update, remove
