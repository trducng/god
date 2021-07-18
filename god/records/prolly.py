"""Implementation of the prolly tree data structure

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


def encode_json(items: list) -> str:
    """Encode the list of dictionary to newline"""
    result = ["["]
    if items:
        for item in items[:-1]:
            result.append(f"{json.dumps(item, sort_keys=True)},")
        result.append(f"{json.dumps(item, sort_keys=True)}")
    result.append("]")

    return "\n".join(result)


def binary_search_key_non_leaf_node(
    key: str, child_nodes: list, start: int = None, end: int = None
) -> str:
    """Locate child node that contain keys

    This algorithm compare betweek `key` and `child_nodes[][1]`

    Args:
        key: the key to search
        child_nodes: each item contains hash and key boundary
        start: the start index to search in `child_nodes`
        end: the end index to search in `child_nodes`

    Returns:
        the hash of child node
    """
    start = 0 if start is None else start
    end = len(child_nodes) - 1 if end is None else end

    if child_nodes[start][1] >= key:
        return child_nodes[start][0]

    if child_nodes[end][1] <= key:
        return child_nodes[end][0]

    if start + 1 == end:
        return child_nodes[end][0]

    middle = (start + end) // 2
    if child_nodes[middle][1] > key:
        return binary_search_key_non_leaf_node(key, child_nodes, start, middle)
    else:
        return binary_search_key_non_leaf_node(key, child_nodes, middle, end)


def binary_search(item, values: list, start: int = None, end: int = None) -> int:
    """Retrieve the index of `item` in list of `values`

    This function assumes that `values` is sorted in increasing order

    Args:
        item: the item to search
        values: the list of item to search from
        start: the start index from `values` to search
        end: the end index from `values` to search

    Returns:
        the index of `item` in `values` or None if not found
    """
    start = 0 if start is None else start
    end = len(values) - 1 if end is None else end

    if values[start] > item:
        return

    if values[end] < item:
        return

    if (start == end) or (end == start + 1):
        if values[start] == item:
            return start
        if values[end] == item:
            return end
        return

    middle = (start + end) // 2
    if values[middle] == item:
        return middle
    elif values[middle] > item:
        return binary_search(item, values, start, middle)
    else:
        return binary_search(item, values, middle, end)


def get_keys_values(keys: str, leaf_nodes: list) -> dict:
    """Search content in the leaf node

    Args:
        key: the key to search
        leaf_nodes: each item contains key and values
        start: the start index to search in `child_nodes`
        end: the end index to search in `child_nodes`

    Returns:
        the value of key or None if nothing match
    """
    leaf_keys = [list(each.keys())[0] for each in leaf_nodes]
    result = {}
    for key in keys:
        key_idx = binary_search(key, leaf_keys)
        result[key] = leaf_nodes[key_idx][key] if key_idx else None

    return result


def prolly_create(items: list, tree_dir: str, leaf_dir: str) -> str:
    """Create a prolly tree containing the items

    Instead of using a rolling hash, we can use the hash of the key, as by our usage,
    the key is the primary key used to index the information.

    Args:
        items: each item is a dict with format `{KEY: {{col1, val1}, {col2, val2}}}`
        tree_dir: the output directory to store the tree

    Returns:
        The location of the root node
    """
    items = sorted(items, key=lambda obj: list(obj.keys())[0])

    # handle leaf nodes
    last_idx = 0
    nodes = []
    for idx, each_item in enumerate(items):
        each_key = list(each_item.keys())[0]
        key_hash = sha256(each_key.encode()).hexdigest()
        if key_hash[:3] == "000":
            content = encode_json(items[last_idx : idx + 1])
            hash_value = sha256(content.encode()).hexdigest()
            nodes.append((hash_value, each_key))
            with Path(leaf_dir, hash_value).open("w") as fo:
                fo.write(content)
            last_idx = idx + 1

    if last_idx != len(items):
        each_key = list(items[-1].keys())[0]
        key_hash = sha256(each_key.encode()).hexdigest()
        content = encode_json(items[last_idx:])
        hash_value = sha256(content.encode()).hexdigest()
        nodes.append((hash_value, each_key))
        with Path(leaf_dir, hash_value).open("w") as fo:
            fo.write(content)

    # handle non-leaf node
    while len(nodes) > 1:
        new_nodes = []
        last_idx = 0
        for idx, node in enumerate(nodes):
            key_hash = sha256(node[0].encode()).hexdigest()
            if key_hash[:3] == "000":
                content = encode_json(nodes[last_idx : idx + 1])
                hash_value = sha256(content.encode()).hexdigest()
                new_nodes.append((hash_value, node[1]))
                with Path(tree_dir, hash_value).open("w") as fo:
                    fo.write(content)
                last_idx = idx + 1

        if last_idx != len(nodes):
            node = nodes[-1]
            key_hash = sha256(node[0].encode()).hexdigest()
            content = encode_json(nodes[last_idx:])
            hash_value = sha256(content.encode()).hexdigest()
            new_nodes.append((hash_value, node[1]))
            with Path(tree_dir, hash_value).open("w") as fo:
                fo.write(content)

        nodes = new_nodes

    return nodes[0][0]


# def prolly_locate_inner_most_non_leaf(keys: list, root: str, tree_dir: str) -> dict:
#     """Search for result inside a prolly tree

#     Args:
#         items: each item is a string, denoting a key to search
#         root: the address of tree root
#         tree_dir: the output directory to store the tree

#     Returns:
#         {key: [non-leaf hashes]} where key is the key and non-leaf hashes are from
#             top-most (root) to bottom-most non-leaf nodes
#     """
#     # locate into inner-most non-leaf node
#     result = {}
#     buff = defaultdict(list)

#     if not Path(tree_dir, root).exists():
#         # This is not non-leaf tree, skip
#         return result

#     with Path(tree_dir, root).open("r") as f_in:
#         child_nodes = json.load(f_in)

#     for key in keys:
#         child_hash = binary_search_key_non_leaf_node(key, child_nodes)
#         result[key] = child_hash
#         buff[child_hash].append(key)

#     for child_hash, child_keys in buff.items():
#         result.update(
#             prolly_locate_inner_most_non_leaf(child_keys, child_hash, tree_dir)
#         )

#     return result


def get_paths_to_leaf(keys: list, root: str, tree_dir: str) -> dict:
    """Search for result inside a prolly tree

    Args:
        items: each item is a string, denoting a key to search
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

    for key in keys:
        child_hash = binary_search_key_non_leaf_node(key, child_nodes)
        buff[child_hash].append(key)

    for child_hash, child_keys in buff.items():
        sub_result = get_paths_to_leaf(child_keys, child_hash, tree_dir)
        for child_key, nodes in sub_result.items():
            result[child_key] += nodes

    return result


def prolly_locate(keys: list, root: str, tree_dir: str, leaf_dir: str) -> dict:
    """Search for result inside a prolly tree

    Args:
        items: each item is a string, denoting a key to search
        root: the address of tree root
        tree_dir: the output directory to store the tree
        leaf_dir: the directory containing leaf nodes

    Returns:
        List of items that match, each item is a dict that has following structure
            {key: {col1: val1, col2: val2}} or {key: None} if key does not exist
    """
    prolly_paths = get_paths_to_leaf(keys, root, tree_dir)

    buff = defaultdict(list)
    for key, leaf_hash in prolly_paths.items():
        buff[leaf_hash[-1]].append(key)

    result = {}
    for leaf_hash, buff_keys in buff.items():
        with Path(leaf_dir, leaf_hash).open("r") as f_in:
            leaf_contents = json.load(f_in)
        result.update(get_keys_values(buff_keys, leaf_contents))

    return result


def prolly_update(items: list, root: str, tree_dir: str, leaf_dir: str) -> None:
    """Update the tree.

    This method assumes that each value in `items` relates involves updating column
    value, not adding or deleting columns.

    Args:
        items: each item is a dict with format `{KEY: {{col1, val1}, {col2, val2}}}`
        root: the address of tree root
        tree_dir: the output directory to store the tree
        leaf_dir: the directory containing leaf nodes
    """
    prolly_paths = get_paths_to_leaf(items, root, tree_dir)  # TODO items must be keys
    buff = defaultdict(list)
    for key, leaf_hash in prolly_paths.items():
        buff[leaf_hash[-1]].append(key)


def prolly_insert(items: list, root: str, tree_dir: str) -> None:
    """Insert items into the tree

    Args:
        items: each item is a dict with format `{key: [(col1, val1), (col2, val2)]}`
        root: the address of tree root
        tree_dir: the output directory to store the tree
    """
    pass


def prolly_delete(items: list, root: str, tree_dir: str) -> None:
    """Insert items into the tree

    Args:
        items: each item is a dict with format `{key: [(col1, val1), (col2, val2)]}`
        root: the address of tree root
        tree_dir: the output directory to store the tree
    """
    pass


if __name__ == "__main__":
    ## Prolly create
    # with open("/home/john/temp/test/example.json", "r") as f_in:
    #     items = json.load(f_in)
    # result = prolly_create(
    #     items, "/home/john/temp/test/prolly", "/home/john/temp/test/prolly/content"
    # ) # print(result)

    ## test binary search
    # with open('/home/john/temp/test/prolly/f48e6ad030312a423e53563eb5dee230f191935e59a808339eaf3068867f7f78', 'r') as fi:
    #     data = json.load(fi)

    # smallest cases
    # result =  binary_search_key_non_leaf_node("275ea4ca485241d1971c3a16fdfeb268", data)
    # print(result)

    # smallest-equal
    # result =  binary_search_key_non_leaf_node("375ea4ca485241d1971c3a16fdfeb268", data)
    # print(result)

    # middle match
    # result =  binary_search_key_non_leaf_node("a20b38c9d8044a2d9d603c918bebb835", data)
    # print(result)

    # middle case
    # result =  binary_search_key_non_leaf_node("d2ef13332c23414487260f8aaa8a5aed", data)
    # print(result)

    # largest-equal cases
    # result =  binary_search_key_non_leaf_node("fd24d94a2c71423f8197b85f71762a99", data)
    # print(result)

    # largest cases
    # result =  binary_search_key_non_leaf_node("fd24d94a2c71423f8197b85f71762b99", data)
    # print(result)

    # ## Prolly locate innermost non-leaf
    result = get_paths_to_leaf(
        [
            "106ff384f7294d86a8f830c4fbfffe6b",
            "a733126211b84c529f02a4298b32a9d7",
            "e3ca88c9ad794b86a1e366e3b78b77ff",
            "79133206bd9c48dd89b1fcc9583c9ffd",
        ],
        root="f48e6ad030312a423e53563eb5dee230f191935e59a808339eaf3068867f7f78",
        tree_dir="/home/john/temp/test/prolly",
    )
    from pprint import pprint

    pprint(result)

    ## Prolly locate values
    # result = prolly_locate(
    #     [
    #         "106ff384f7294d86a8f830c4fbfffe6b",
    #         "a733126211b84c529f02a4298b32a9d7",
    #         "e3ca88c9ad794b86a1e366e3b78b77ff",
    #         "79133206bd9c48dd89b1fcc9583c9ffd",
    #     ],
    #     root="f48e6ad030312a423e53563eb5dee230f191935e59a808339eaf3068867f7f78",
    #     tree_dir="/home/john/temp/test/prolly",
    #     leaf_dir="/home/john/temp/test/prolly/content",
    # )
    # from pprint import pprint

    # pprint(result)
