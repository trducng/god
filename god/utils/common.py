import hashlib


def get_string_hash(string):
    """Get string hash

    # Args:
        string <str>: the input string

    # Returns:
        <str>: hash value of the string
    """
    return hashlib.sha256(string.encode()).hexdigest()
