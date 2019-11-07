#!/usr/bin/env python3
import datetime
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from shutil import copystat
from threading import Thread, Event, currentThread
from time import sleep
from typing import List

import xxhash

from ocopy.file_info import FileInfo
from ocopy.hash import multi_xxhash_check, write_xxhash_summary
from ocopy.mhl import write_mhl, find_mhl, get_hash_from_mhl
from ocopy.progress import get_progress_queue
from ocopy.utils import threaded, folder_size


class CopyTreeError(OSError):
    """
    Raised by recursive copytree
    """

    pass


class VerificationError(OSError):
    """
    Raised if checksums for copied files do not match
    """

    pass


@dataclass
class ErrorListEntry:
    """
    Used to store errors while continuing the recursive copytree
    """

    source: Path
    destinations: List[Path]
    error_message: str


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

    with ThreadPoolExecutor(max_workers=len(destinations)) as executor:
        futures = [executor.submit(writer, queues[i], d) for i, d in enumerate(destinations)]

        x = xxhash.xxh64()
        progress_queue = get_progress_queue()

        with open(src_file, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                for q in queues:
                    q.put(chunk)

                if not chunk:
                    break

                x.update(chunk)
                if progress_queue:
                    progress_queue.put((src_file, len(chunk)))

        for future in as_completed(futures):
            future.result()

    for q in queues:
        q.join()

    for d in destinations:
        copystat(src_file, d)

    return x.hexdigest()


def copytree(
    source: Path, destinations: List[Path], overwrite=False, verify=True, skip_existing=False
) -> List[FileInfo]:
    """Recursively copy a source directory to multiple destinations"""

    for d in destinations:
        d.mkdir(parents=True, exist_ok=True)

    ignored_files = [".DS_Store", ".fseventsd"]

    file_infos = []
    errors = []

    for src_path in source.glob("*"):
        if _is_cancelled():
            break

        if src_path.name in ignored_files:
            continue
        dst_paths = [d / src_path.name for d in destinations]
        try:
            if src_path.is_dir():
                file_infos += copytree(src_path, dst_paths, overwrite, verify, skip_existing)
            else:
                file_hash = verified_copy(src_path, dst_paths, overwrite, verify, skip_existing)
                stat = src_path.stat()
                file_infos.append(FileInfo(src_path, file_hash, stat.st_size, stat.st_mtime))

        # Catch the CopyTreeError from the recursive copytree so that we can continue with other files
        except CopyTreeError as err:
            errors.extend(err.args[0])
        except OSError as why:
            errors.append(ErrorListEntry(src_path, dst_paths, str(why)))

    for d in destinations:
        copystat(source, d)

    if errors:
        raise CopyTreeError(errors)

    return file_infos


def verified_copy(src_file: Path, destinations: List[Path], overwrite=False, verify=True, skip_existing=False) -> str:
    """
    Copies one file to multiple destinations.

    - Creates temporary file on destinations
    - Calculates checksum of source during copy
    - Re-Reads source and all destinations and make sure all checksums match
    """
    to_do_destinations = destinations.copy()

    for destination in destinations:
        if destination.exists():
            if (
                skip_existing
                and src_file.stat().st_size == destination.stat().st_size
                and abs(src_file.stat().st_mtime - destination.stat().st_mtime) <= 2
            ):
                try:
                    currentThread().skipped_files += 1
                except AttributeError:
                    pass
                to_do_destinations.remove(destination)
            elif overwrite:
                destination.unlink()
            else:
                raise FileExistsError(f"{destination.as_posix()} exists!")

    if to_do_destinations:
        tmp_destinations = [d.with_name(d.name + ".copy_in_progress") for d in to_do_destinations]
        try:
            file_hash = copy(src_file, tmp_destinations)

            # Verify source and destinations
            if not verify or file_hash == multi_xxhash_check(tmp_destinations + [src_file]):
                for tmp in tmp_destinations:
                    tmp.rename(tmp.with_name(tmp.name.replace(".copy_in_progress", "")))
                return file_hash
            else:
                raise VerificationError(f"Verification failed for {src_file}")
        finally:
            for tmp in tmp_destinations:
                try:
                    tmp.unlink()
                except FileNotFoundError:
                    pass
    else:
        mhl_file = find_mhl(destinations[0])
        try:
            hash_sum = get_hash_from_mhl(mhl_file.read_text(), destinations[0].relative_to(mhl_file.parent))
            return hash_sum
        except AttributeError:
            return ""


def copy_and_seal(source: Path, destinations: List[Path], overwrite=False, verify=True, skip_existing=False):
    destinations = [d / source.name for d in destinations]

    start = datetime.datetime.utcnow()
    file_infos = copytree(source, destinations, overwrite=overwrite, verify=verify, skip_existing=skip_existing)

    write_mhl(destinations, file_infos, source, start)
    write_xxhash_summary(destinations, file_infos)


def _is_cancelled() -> bool:
    try:
        return currentThread().cancelled
    except AttributeError:
        return False


class CopyJob(Thread):
    total_size: int
    total_done: int
    finished: bool
    errors: List[ErrorListEntry]

    def __init__(
        self, source: Path, destinations: List[Path], overwrite=False, verify=True, skip_existing=False, auto_start=True
    ):
        super().__init__()
        self.daemon = True
        self.skipped_files = 0
        self.errors = []
        self._progress_queue = Queue()
        self._cancel = Event()

        self.source = source
        self.destinations = destinations
        self.overwrite = overwrite
        self.verify = verify
        self.skip_existing = skip_existing

        self.total_size = folder_size(source)
        self.todo_size = self.total_size * (2 if self.verify else 1)
        self.total_done = 0
        self.current_item = None
        self.finished = False

        if auto_start:
            self.start()

    def cancel(self):
        self._cancel.set()

    @property
    def cancelled(self) -> bool:
        return self._cancel.isSet()

    @threaded
    def _progress_reader(self):
        while not (self.finished or self.cancelled):
            file_path, done = self._progress_queue.get()
            self.current_item = Path(file_path).name
            self.total_done += done

    @property
    def percent_done(self) -> int:
        return round(100 / self.todo_size * self.total_done)

    @property
    def progress(self) -> str:
        for i in range(1, 101):
            while self.percent_done < i:
                sleep(0.1)
            yield self.current_item

    def run(self):
        if self.cancelled:
            self.finished = True
            return

        self._progress_reader()

        try:
            copy_and_seal(
                source=self.source,
                destinations=self.destinations,
                overwrite=self.overwrite,
                verify=self.verify,
                skip_existing=self.skip_existing,
            )

        except CopyTreeError as e:
            self.errors = e.args[0]

        self.finished = True
        self.total_done = self.todo_size
