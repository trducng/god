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
from hashlib import sha256
from pathlib import Path


def encode_json(items: list) -> str:
    """Encode the list of dictionary to newline"""
    result = ["["]
    for item in items:
        result.append(f"{json.dumps(item, sort_keys=True)},")
    result.append("]")

    return "\n".join(result)


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


def prolly_search(items: list, root: str, tree_dir: str) -> list:
    """Search for result inside a prolly tree

    Args:
        items: each item is a string, denoting a key to search
        root: the address of tree root
        tree_dir: the output directory to store the tree

    Returns:
        List of items that match, each item is a dict that has following structure
            {(key, hashval): [(col1, val1), (col2, val2)]}
    """
    pass


def prolly_update(items: list, root: str, tree_dir: str) -> None:
    """Update the tree

    Args:
        items: each item is a dict with format `{KEY: {{col1, val1}, {col2, val2}}}`
        root: the address of tree root
        tree_dir: the output directory to store the tree
    """
    pass


def prolly_insert(items: list, root: str, tree_dir: str) -> None:
    """Insert items into the tree

    Args:
        items: each item is a dict with format `{key: [(col1, val1), (col2, val2)]}`
        root: the address of tree root
        tree_dir: the output directory to store the tree
    """
    pass


if __name__ == "__main__":
    with open("/home/john/temp/test/example.json", "r") as f_in:
        items = json.load(f_in)
    result = prolly_create(
        items, "/home/john/temp/test/prolly", "/home/john/temp/test/prolly/content"
    )
    print(result)
