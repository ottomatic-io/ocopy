from concurrent import futures
from functools import partial
from pathlib import Path
from queue import Queue
from typing import List

import xxhash

from ocopy.file_info import FileInfo
from ocopy.progress import get_progress_queue


def get_hash(file_path: Path, progress_queue: Queue = None, total_files: int = 1) -> str:
    x = xxhash.xxh64()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            x.update(chunk)
            if progress_queue:
                progress_queue.put(
                    (
                        file_path.with_name(file_path.name.replace(".copy_in_progress", "") + " (verify)"),
                        len(chunk) / total_files,
                    )
                )

    return x.hexdigest()


def multi_xxhash_check(filenames: List[Path]) -> str:
    with futures.ThreadPoolExecutor(max_workers=len(filenames)) as executor:
        unique_file_hashes = {
            file_hash
            for file_hash in executor.map(
                partial(get_hash, progress_queue=get_progress_queue(), total_files=len(filenames)), filenames
            )
        }

    return unique_file_hashes.pop() if len(unique_file_hashes) == 1 else "hashes_do_not_match"


def write_xxhash_summary(destinations: List[Path], file_infos: List[FileInfo]):
    xxhash_info = "\n".join(f"{f.file_hash} {Path(f.source).name}" for f in file_infos) + "\n"
    for d in destinations:
        (d / "xxHash.txt").write_text(xxhash_info)
