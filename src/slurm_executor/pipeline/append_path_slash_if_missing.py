def append_path_slash_if_missing(path: str) -> str:
    if not path.endswith("/"):
        return path + "/"
    return path
