import os
from pathlib import Path
from typing import Optional


def find_dot_xxhash(file_path: Path) -> Optional[Path]:
    """
    Finds the last created .xxhash which is closest in the folder hierarchy
    """
    if os.path.ismount(file_path.as_posix()):
        return

    dot_xxhash_in_parent = sorted(list(file_path.parent.glob("*.xxhash")))
    if dot_xxhash_in_parent:
        return dot_xxhash_in_parent[-1]
    else:
        return find_dot_xxhash(file_path.parent)


def get_hash_from_dot_xxhash(dot_xxhash: str, file_path: Path) -> str:
    """
    Reads the xxhash for a file from dot_xxhash string
    """
    if not dot_xxhash:
        return ""

    for line in dot_xxhash.splitlines():
        if line.startswith("#"):
            continue

        file_hash, relative_path = line.split()

        if str(file_path) == relative_path:
            return file_hash

    return ""
