import logging
import os
from pathlib import Path

from ocopy.ignored import is_ignored_basename

logger = logging.getLogger(__name__)


def get_signatures(path: Path) -> set:
    signatures = set()

    for root, dirs, files in os.walk(path.as_posix()):
        dirs[:] = [d for d in dirs if not is_ignored_basename(d)]
        files = [f for f in files if not is_ignored_basename(f)]

        for filename in files:
            filepath = os.path.join(root, filename)
            try:
                signatures.add((filename, os.path.getsize(filepath)))
            except OSError:
                logger.warning(f"Could not get signature for {filepath}")
                signatures.add((filename, -1))

    return signatures


def get_missing(src: str, dst: str) -> tuple[list[str], int]:
    logger.info(f"Searching all files from {src} in {dst}")
    src_path = Path(src)

    missing = get_signatures(src_path)
    count = len(missing)
    logger.info("Found %d files on %s", count, src_path)

    endings = tuple({os.path.splitext(m[0])[1] or m[0] for m in missing})

    for root, dirs, files in os.walk(dst):
        dirs.sort(reverse=True)

        dirs[:] = [d for d in dirs if not is_ignored_basename(d)]
        files = [f for f in files if not is_ignored_basename(f) and f.endswith(endings)]

        for filename in files:
            filepath = os.path.join(root, filename)
            try:
                missing.difference_update([(filename, os.path.getsize(filepath))])
            except OSError:
                logger.warning(f"Could not get size for {filepath}")

    missing_files = [m[0] for m in missing]
    logger.info(f"Missing files: {missing_files}")

    return missing_files, count
