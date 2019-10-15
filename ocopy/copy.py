#!/usr/bin/env python3
import datetime
from pathlib import Path
from queue import Queue
from shutil import copystat
from threading import Thread
from typing import List

import xxhash

from ocopy.file_info import FileInfo
from ocopy.hash import multi_xxhash_check, write_xxhash_summary
from ocopy.mhl import write_mhl
from ocopy.progress import progress_queue
from ocopy.utils import threaded


class CopyFailedException(Exception):
    pass


def copy(src_file: Path, destinations: List[Path], chunk_size: int = 1024 * 1024) -> str:
    """
    Copies one file to multiple destinations chunk by chunk while calculating the checksum
    """
    queues = [Queue(maxsize=10) for _ in destinations]

    def writer(queue: Queue, file_path: Path):
        with open(file_path, "wb") as dest_f:
            while True:
                write_chunk = queue.get()
                if not write_chunk:
                    queue.task_done()
                    break
                dest_f.write(write_chunk)
                queue.task_done()

    writers = [Thread(target=writer, args=(queues[i], d)) for i, d in enumerate(destinations)]

    for w in writers:
        w.daemon = True
        w.start()

    x = xxhash.xxh64()

    with open(src_file, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            for q in queues:
                q.put(chunk)

            if not chunk:
                break

            x.update(chunk)
            progress_queue.put((src_file, len(chunk)))

    for q in queues:
        q.join()

    for w in writers:
        w.join()

    for d in destinations:
        copystat(src_file, d)

    return x.hexdigest()


def copytree(source: Path, destinations: List[Path], overwrite=False, verify=True) -> List[FileInfo]:
    """Based on shutil.copytree"""

    class Error(Exception):
        """Base class for exceptions in this module."""

        pass

    for d in destinations:
        d.mkdir(parents=True, exist_ok=True)

    file_infos = []
    errors = []

    for src_path in source.glob('*'):
        dst_paths = [d / src_path.name for d in destinations]
        try:
            if src_path.is_dir():
                file_infos += copytree(src_path, dst_paths, overwrite=overwrite, verify=verify)
            else:
                file_hash = verified_copy(src_path, dst_paths, overwrite=overwrite, verify=verify)
                stat = src_path.stat()
                file_infos.append(FileInfo(src_path, file_hash, stat.st_size, stat.st_mtime))

        # catch the Error from the recursive copytree so that we can
        # continue with other files
        except Error as err:
            errors.extend(err.args[0])
        except EnvironmentError as why:
            errors.append((src_path, dst_paths, str(why)))

    for d in destinations:
        copystat(source, d)

    if errors:
        raise Error(errors)

    return file_infos


def verified_copy(src_file: Path, destinations: List[Path], overwrite=False, verify=True) -> str:
    """
    Copies one file to multiple destinations.

    - Creates temporary file on destinations
    - Calculates checksum of source during copy
    - Re-Reads source and all destinations and make sure all checksums match
    """
    if not overwrite:
        for d in destinations:
            if d.exists():
                raise FileExistsError(f"{d.as_posix()} exists!")

    tmp_destinations = [d.with_name(d.name + ".copy_in_progress") for d in destinations]
    file_hash = copy(src_file, tmp_destinations)

    # Verify source and destinations
    if not verify or file_hash == multi_xxhash_check(tmp_destinations + [src_file]):
        for tmp in tmp_destinations:
            tmp.rename(tmp.with_name(tmp.name.replace(".copy_in_progress", "")))
        return file_hash
    else:
        for tmp in tmp_destinations:
            tmp.unlink()

        raise CopyFailedException(src_file)


@threaded
def copy_and_seal(source: Path, destinations: List[Path], overwrite=False, verify=True):
    destinations = [d / source.name for d in destinations]

    start = datetime.datetime.utcnow()
    file_infos = copytree(source, destinations, overwrite=overwrite, verify=verify)

    write_mhl(destinations, file_infos, source, start)
    write_xxhash_summary(destinations, file_infos)

    progress_queue.put(('finished', -1))
