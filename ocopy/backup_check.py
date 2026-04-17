import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def get_signatures(path: Path) -> set:
    signatures = set()
    ignored_dirs = ["Backups.backupdb", "System Volume Information"]

    for root, dirs, files in os.walk(path.as_posix()):
        files = [f for f in files if f[0] != "."]
        dirs[:] = [d for d in dirs if d[0] != "." and d not in ignored_dirs]

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
    ignored_dirs = ["Backups.backupdb", "System Volume Information"]

    for root, dirs, files in os.walk(dst):
        dirs.sort(reverse=True)

        files = [f for f in files if f[0] != "." and f.endswith(endings)]
        dirs[:] = [d for d in dirs if d[0] != "." and d not in ignored_dirs]

        for filename in files:
            filepath = os.path.join(root, filename)
            try:
                missing.difference_update([(filename, os.path.getsize(filepath))])
            except OSError:
                logger.warning(f"Could not get size for {filepath}")

    missing_files = [m[0] for m in missing]
    logger.info(f"Missing files: {missing_files}")

    return missing_files, count
