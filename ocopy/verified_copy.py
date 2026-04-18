#!/usr/bin/env python3
import contextlib
import datetime
import time
from collections.abc import Iterator
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from shutil import copystat
from threading import Event, Thread, current_thread
from time import sleep

import xxhash

from ocopy.ascmhl_seal import ASCMHLSealError, seal_ascmhl_destinations
from ocopy.file_info import FileInfo
from ocopy.hash import find_hash, multi_xxhash_check
from ocopy.ignored import ignored_paths
from ocopy.mhl import write_mhl
from ocopy.progress import get_progress_queue
from ocopy.utils import folder_size, threaded


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
    destinations: list[Path]
    error_message: str


def copy(src_file: Path, destinations: list[Path], chunk_size: int = 1024 * 1024) -> str:
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
    source: Path, destinations: list[Path], overwrite=False, verify=True, skip_existing=False
) -> list[FileInfo]:
    """Recursively copy a source directory to multiple destinations"""

    for d in destinations:
        d.mkdir(parents=True, exist_ok=True)

    file_infos = []
    errors = []

    for src_path in source.glob("*"):
        if _is_cancelled():
            break

        if src_path.name in ignored_paths:
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

    if errors:
        raise CopyTreeError(errors)

    return file_infos


def verified_copy(src_file: Path, destinations: list[Path], overwrite=False, verify=True, skip_existing=False) -> str:
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
                thread = current_thread()
                thread.skipped_files = getattr(thread, "skipped_files", 0) + 1  # ty: ignore[unresolved-attribute]
                to_do_destinations.remove(destination)
            elif overwrite:
                destination.unlink()
            else:
                raise FileExistsError(f"{destination.as_posix()} exists!")

    if to_do_destinations:
        tmp_destinations = [d.with_name(d.name + ".copy_in_progress") for d in to_do_destinations]
        try:
            file_hash = copy(src_file, tmp_destinations)
            present_hash = find_hash(src_file)

            if not present_hash:
                to_verify = [*tmp_destinations, src_file]
            else:
                to_verify = tmp_destinations

                if present_hash != file_hash:
                    raise VerificationError(
                        f"Verification failed for {src_file}. xxHash present on source medium is not correct"
                    )

            # Verify
            if not verify or file_hash == multi_xxhash_check(to_verify):
                for tmp in tmp_destinations:
                    tmp.rename(tmp.with_name(tmp.name.replace(".copy_in_progress", "")))
                return file_hash
            else:
                raise VerificationError(f"Verification failed for {src_file}")
        finally:
            for tmp in tmp_destinations:
                with contextlib.suppress(FileNotFoundError):
                    tmp.unlink()
    else:
        # All destinations were skipped; surface whichever hash the destination already advertises.
        # ``find_hash`` walks ASC MHL first (innermost-P), then legacy ``.mhl``, then dot-xxhash sidecars.
        return find_hash(destinations[0]) or ""


def copy_and_seal(
    source: Path,
    destinations: list[Path],
    overwrite=False,
    verify=True,
    skip_existing=False,
    mhl=True,
    legacy_mhl=False,
):
    destinations = [d / source.name for d in destinations]

    file_infos = copytree(source, destinations, overwrite=overwrite, verify=verify, skip_existing=skip_existing)

    if mhl:
        if legacy_mhl:
            start = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
            write_mhl(destinations, file_infos, source, start)
        else:
            try:
                seal_ascmhl_destinations(destinations, source, file_infos)
            except ASCMHLSealError as err:
                raise CopyTreeError([ErrorListEntry(source, destinations, str(err))]) from err


def _is_cancelled() -> bool:
    return getattr(current_thread(), "cancelled", False)


class CopyJob(Thread):
    total_size: int
    total_done: int
    finished: bool
    errors: list[ErrorListEntry]

    def __init__(
        self,
        source: Path,
        destinations: list[Path],
        overwrite=False,
        verify=True,
        skip_existing=False,
        mhl=True,
        legacy_mhl=False,
        auto_start=True,
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
        self.mhl = mhl
        self.legacy_mhl = legacy_mhl

        self.total_size = folder_size(source)
        self.todo_size = self.total_size * (2 if self.verify else 1)
        self.total_done = 0
        self.current_item = None
        self.finished = False
        self._start_time = time.time()

        if auto_start:
            self.start()

    def cancel(self):
        self._cancel.set()

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

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
    def speed(self) -> float:
        now = time.time()
        return (self.total_done / 2 if self.verify else self.total_done) / (now - self._start_time)

    @property
    def progress(self) -> Iterator[str | None]:
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
                mhl=self.mhl,
                legacy_mhl=self.legacy_mhl,
            )

        except CopyTreeError as e:
            self.errors = e.args[0]

        self.finished = True
        self.total_done = self.todo_size
