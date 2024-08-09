import re


def is_unix_path(path):
    pattern = r"^\/[\w\/]*"
    if re.match(pattern, path):
        return True
    return False
