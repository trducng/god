import hashlib
import json


def one_line_sorted_json(items: list) -> str:
    """Encode the list of dictionary separated by newline

    Purposes:
        - human readable: separating each dictionary inside `items` by newline
        - unique: sort the dictionary keys for uniqueness

    Args:
        items: the list of dictionaries content

    Returns:
        the string representation of `items` that can be written to a text file as
            valid JSON
    """
    result = ["["]
    if items:
        for item in items[:-1]:
            result.append(f"{json.dumps(item, sort_keys=True)},")
        result.append(f"{json.dumps(items[-1], sort_keys=True)}")
    result.append("]")

    return "\n".join(result)


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


def get_string_hash(string):
    """Get string hash

    # Args:
        string <str>: the input string

    # Returns:
        <str>: hash value of the string
    """
    return hashlib.sha256(string.encode()).hexdigest()
