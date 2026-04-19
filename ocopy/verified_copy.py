#!/usr/bin/env python3
from __future__ import annotations

import contextlib
import datetime
import os
import time
from collections.abc import Callable, Iterator
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue
from shutil import copystat
from threading import Event, Thread
from time import sleep

import xxhash

from ocopy.ascmhl_seal import ASCMHLSealError, seal_ascmhl_destinations
from ocopy.checkpoint import Checkpoint
from ocopy.file_info import FileInfo
from ocopy.hash import find_hash, multi_xxhash_check
from ocopy.ignored import is_ignored_basename
from ocopy.mhl import write_mhl
from ocopy.progress import ProgressPhase, ProgressUpdate, get_progress_queue
from ocopy.utils import folder_size, threaded

CancelToken = Callable[[], bool]
"""Cancellation signal callable: returns True once the caller should stop."""


class CopyTreeError(OSError):
    """Raised by recursive ``copytree`` when one or more files failed to copy."""


class VerificationError(OSError):
    """Raised when source and destination checksums disagree."""


@dataclass
class ErrorListEntry:
    """One failure captured while the traversal continues through sibling files."""

    source: Path
    destinations: list[Path]
    error_message: str


@dataclass
class CopyResult:
    """Structured outcome of a :func:`copy_and_seal` invocation.

    ``CopyJob`` wraps this and exposes it through read-only properties; tests and
    library callers can treat it as the single source of truth for the run.
    """

    file_infos: list[FileInfo] = field(default_factory=list)
    skipped_files: int = 0
    cancelled: bool = False
    checkpoint_paths: list[Path] = field(default_factory=list)


@dataclass
class _CopyState:
    """Per-run state threaded from ``copy_and_seal`` down to ``verified_copy``.

    Uses a plain dataclass (not a context manager or thread-local) so the
    classification/copy logic stays pure: every side channel is an explicit
    field on this object.
    """

    cancel_token: CancelToken
    checkpoints: list[Checkpoint]
    source_tree_root: Path
    need_integrity: bool
    skipped_files: int = 0


def _never_cancelled() -> bool:
    return False


def copy(src_file: Path, destinations: list[Path], chunk_size: int = 1024 * 1024) -> str:
    """Copy one file to multiple destinations chunk by chunk, returning its xxh64."""
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
                    progress_queue.put(ProgressUpdate(ProgressPhase.COPY, src_file, len(chunk)))

        for future in as_completed(futures):
            future.result()

    for q in queues:
        q.join()

    for d in destinations:
        copystat(src_file, d)

    return x.hexdigest()


def _default_state(source_root: Path, verify: bool) -> _CopyState:
    """Default state for callers (tests, library users) that skip the ``state=`` kwarg."""
    return _CopyState(
        cancel_token=_never_cancelled,
        checkpoints=[],
        source_tree_root=source_root,
        need_integrity=verify,
    )


def copytree(
    source: Path,
    destinations: list[Path],
    overwrite: bool = False,
    verify: bool = True,
    skip_existing: bool = False,
    *,
    state: _CopyState | None = None,
) -> list[FileInfo]:
    """Recursively copy ``source`` to each of ``destinations``.

    Children of each directory are visited in lexicographic order by basename
    (depth-first), so copy order does not depend on filesystem iteration order.

    ``state`` is an internal plumbing parameter; direct callers (tests, library use)
    may omit it and receive default "no cancellation, no checkpoints" behavior.
    """
    if state is None:
        state = _default_state(source.resolve(), verify)

    for d in destinations:
        d.mkdir(parents=True, exist_ok=True)

    file_infos: list[FileInfo] = []
    errors: list[ErrorListEntry] = []

    for src_path in sorted(source.glob("*"), key=lambda p: p.name):
        if state.cancel_token():
            break

        if is_ignored_basename(src_path.name):
            continue
        dst_paths = [d / src_path.name for d in destinations]
        try:
            if src_path.is_dir():
                file_infos += copytree(
                    src_path,
                    dst_paths,
                    overwrite,
                    verify,
                    skip_existing,
                    state=state,
                )
            else:
                file_hash = verified_copy(
                    src_path,
                    dst_paths,
                    overwrite,
                    verify,
                    skip_existing,
                    state=state,
                )
                stat = src_path.stat()
                file_infos.append(FileInfo(src_path, file_hash, stat.st_size, stat.st_mtime))

        # Continue past per-file failures so one bad file doesn't abort the tree.
        except CopyTreeError as err:
            errors.extend(err.args[0])
        except OSError as why:
            errors.append(ErrorListEntry(src_path, dst_paths, str(why)))

    if errors:
        raise CopyTreeError(errors)

    return file_infos


def _classify_destinations(
    destinations: list[Path],
    src_stat_fn: Callable[[], os.stat_result],
    overwrite: bool,
    skip_existing: bool,
    need_integrity: bool,
) -> tuple[list[int], list[int], list[int], list[str]]:
    """Split destinations into ``(to_copy, to_verify, trusted, trusted_hashes)`` buckets.

    ``src_stat_fn`` is a zero-arg callable so we don't ``stat()`` the source unless
    a destination actually exists and needs metadata comparison. Tests that mock
    the filesystem rely on this deferral; so does the general principle of not
    stat'ing files we don't need to touch.

    Raises ``FileExistsError`` eagerly if a destination is in the way and neither
    ``skip_existing`` nor ``overwrite`` is set.
    """
    copy_idx: list[int] = []
    verify_idx: list[int] = []
    trusted_idx: list[int] = []
    trusted_hashes: list[str] = []

    for i, dest in enumerate(destinations):
        if not dest.exists():
            copy_idx.append(i)
            continue

        dst_stat = dest.stat()
        src_stat = src_stat_fn()
        meta_ok = (
            skip_existing and src_stat.st_size == dst_stat.st_size and abs(src_stat.st_mtime - dst_stat.st_mtime) <= 2
        )
        if not skip_existing or not meta_ok:
            if overwrite:
                dest.unlink()
                copy_idx.append(i)
            else:
                raise FileExistsError(f"{dest.as_posix()} exists!")
            continue

        if not need_integrity:
            continue

        existing = find_hash(dest)
        if existing:
            trusted_hashes.append(existing)
            trusted_idx.append(i)
        else:
            verify_idx.append(i)

    return copy_idx, verify_idx, trusted_idx, trusted_hashes


def verified_copy(
    src_file: Path,
    destinations: list[Path],
    overwrite: bool = False,
    verify: bool = True,
    skip_existing: bool = False,
    *,
    state: _CopyState | None = None,
) -> str:
    """Copy ``src_file`` to ``destinations`` with integrity guarantees.

    Behavior matrix (see the issue #9 plan for the rationale):

    - Destination missing -> copy into a ``.copy_in_progress`` temp, then rename.
    - Destination present + ``skip_existing`` + size/mtime match + trusted hash -> fast-skip.
    - Destination present + ``skip_existing`` + match + no trusted hash + integrity required -> re-hash.
    - Destination present + not matching -> ``FileExistsError`` unless ``overwrite`` is set.
    - Verification mismatch + ``overwrite`` -> repair once; second mismatch raises.
    """
    if state is None:
        state = _default_state(src_file.parent.resolve(), verify)

    rel_path = src_file.resolve().relative_to(state.source_tree_root.resolve()).as_posix()

    src_stat_cache: os.stat_result | None = None

    def src_stat() -> os.stat_result:
        nonlocal src_stat_cache
        if src_stat_cache is None:
            src_stat_cache = src_file.stat()
        return src_stat_cache

    max_attempts = 2  # initial + at most one repair retry
    for attempt in range(max_attempts):
        copy_idx, verify_idx, trusted_idx, trusted_hashes = _classify_destinations(
            destinations,
            src_stat,
            overwrite=overwrite,
            skip_existing=skip_existing,
            need_integrity=state.need_integrity,
        )

        # Nothing to copy and nothing to re-verify: either every destination is a
        # pure metadata-match skip (caller opted out of integrity), or every
        # destination carries a trusted hash we can simply echo back. The empty
        # ``destinations`` case (pathological but well-defined) also lands here
        # with empty buckets and returns the no-integrity marker.
        if not copy_idx and not verify_idx:
            if trusted_hashes:
                if len(set(trusted_hashes)) > 1:
                    raise VerificationError(f"Conflicting trusted hashes for {src_file}")
                digest = trusted_hashes[0]
                s = src_stat()
                _record_checkpoints(state.checkpoints, rel_path, s.st_size, s.st_mtime, digest)
                state.skipped_files += len(trusted_idx)
                return digest
            state.skipped_files += len(destinations)
            return ""

        tmps = [destinations[i].with_name(destinations[i].name + ".copy_in_progress") for i in copy_idx]
        copy_hash: str | None = None
        if tmps:
            try:
                copy_hash = copy(src_file, tmps)
            except BaseException:
                _cleanup_tmps(tmps)
                raise

        # Build the verification pool lazily; trusted destinations are included whenever
        # we're already running a pool check so a lying manifest doesn't slip through.
        pool: list[Path] = [src_file, *tmps]
        pool.extend(destinations[i] for i in verify_idx)
        pool.extend(destinations[i] for i in trusted_idx)
        need_pool_verify = bool(verify_idx) or (bool(copy_idx) and verify)

        try:
            if not state.need_integrity:
                assert copy_hash is not None
                _rename_tmps(tmps, [destinations[i] for i in copy_idx])
                # Any destination that wasn't in ``copy_idx`` or ``verify_idx`` was a
                # pure metadata-matched skip that never entered the classification lists.
                state.skipped_files += len(destinations) - len(copy_idx)
                return copy_hash

            if need_pool_verify:
                combined = multi_xxhash_check(pool)
                if combined == "hashes_do_not_match":
                    last_attempt = attempt == max_attempts - 1
                    if not overwrite or last_attempt:
                        raise VerificationError(f"Verification failed for {src_file}")
                    _cleanup_tmps(tmps)
                    for dest in destinations:
                        with contextlib.suppress(FileNotFoundError):
                            dest.unlink()
                    continue  # retry from classification
                digest = combined
            else:
                assert copy_hash is not None
                digest = copy_hash

            present_hash = find_hash(src_file)
            if present_hash and present_hash != digest:
                raise VerificationError(
                    f"Verification failed for {src_file}. xxHash present on source medium is not correct"
                )

            _rename_tmps(tmps, [destinations[i] for i in copy_idx])
            s = src_stat()
            _record_checkpoints(state.checkpoints, rel_path, s.st_size, s.st_mtime, digest)
            # ``verify_idx`` destinations were present already and did not receive new bytes,
            # so they count as skipped (just with a paid-for verification read).
            state.skipped_files += len(verify_idx) + len(trusted_idx)
            return digest
        except BaseException:
            _cleanup_tmps(tmps)
            raise

    raise AssertionError("unreachable: verified_copy retry loop exited without returning")


def _record_checkpoints(checkpoints: list[Checkpoint], rel_path: str, size: int, mtime: float, digest: str) -> None:
    for cp in checkpoints:
        cp.record(rel_path, size, mtime, digest)


def _cleanup_tmps(tmps: list[Path]) -> None:
    for tmp in tmps:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()


def _rename_tmps(tmps: list[Path], final_paths: list[Path]) -> None:
    for tmp, final in zip(tmps, final_paths, strict=True):
        tmp.rename(final)


def copy_and_seal(
    source: Path,
    destinations: list[Path],
    overwrite: bool = False,
    verify: bool = True,
    skip_existing: bool = False,
    mhl: bool = True,
    legacy_mhl: bool = False,
    cancel_token: CancelToken | None = None,
) -> CopyResult:
    """Copy ``source`` into each destination and (optionally) seal an ASC MHL.

    The returned :class:`CopyResult` exposes ``skipped_files``, ``cancelled``, and
    ``checkpoint_paths`` so callers don't need to poke at thread attributes.
    Raises :class:`CopyTreeError` if any file failed to copy; in that case the
    caller is expected to consult the exception's error list.
    """
    token = cancel_token or _never_cancelled

    dest_roots = [d / source.name for d in destinations]
    checkpoints = [Checkpoint(root) for root in dest_roots]
    for cp in checkpoints:
        cp.ensure_exists()

    state = _CopyState(
        cancel_token=token,
        checkpoints=checkpoints,
        source_tree_root=source.resolve(),
        need_integrity=mhl or verify,
    )

    file_infos = copytree(
        source,
        dest_roots,
        overwrite=overwrite,
        verify=verify,
        skip_existing=skip_existing,
        state=state,
    )

    result = CopyResult(
        file_infos=file_infos,
        skipped_files=state.skipped_files,
        checkpoint_paths=[cp.path for cp in checkpoints],
    )

    if token():
        result.cancelled = True
        return result

    if mhl:
        if legacy_mhl:
            start = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
            write_mhl(dest_roots, file_infos, source, start)
        else:
            try:
                seal_ascmhl_destinations(dest_roots, source, file_infos)
            except ASCMHLSealError as err:
                raise CopyTreeError([ErrorListEntry(source, dest_roots, str(err))]) from err

    for cp in checkpoints:
        cp.clear()

    return result


class CopyJob(Thread):
    total_size: int
    total_done: float
    finished: bool
    errors: list[ErrorListEntry]
    result: CopyResult

    def __init__(
        self,
        source: Path,
        destinations: list[Path],
        overwrite: bool = False,
        verify: bool = True,
        skip_existing: bool = False,
        mhl: bool = True,
        legacy_mhl: bool = False,
        auto_start: bool = True,
        cancel_token: CancelToken | None = None,
    ):
        super().__init__()
        self.daemon = True
        self.errors = []
        self._progress_queue: Queue[ProgressUpdate] = Queue()
        self._cancel = Event()
        # Allow tests and library users to inject a custom cancellation signal
        # (e.g. a counter-based token that fires mid-tree). Production code uses
        # the built-in Event wired to :meth:`cancel`.
        self._cancel_token: CancelToken = cancel_token if cancel_token is not None else self._cancel.is_set

        self.source = source
        self.destinations = destinations
        self.overwrite = overwrite
        self.verify = verify
        self.skip_existing = skip_existing
        self.mhl = mhl
        self.legacy_mhl = legacy_mhl

        # Pre-compute checkpoint paths so CLI cancel reporting works even before
        # the run thread has had a chance to create the files on disk.
        dest_roots = [d / source.name for d in destinations]
        self.result = CopyResult(checkpoint_paths=[r / Checkpoint.FILENAME for r in dest_roots])

        self.total_size = folder_size(source)
        self.todo_size = self.total_size * (2 if self.verify else 1)
        self.total_done = 0.0
        self.current_item = None
        self.finished = False
        self._start_time = time.time()

        if auto_start:
            self.start()

    def cancel(self) -> None:
        self._cancel.set()

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    @property
    def interrupted_by_cancel(self) -> bool:
        return self.result.cancelled

    @property
    def verified_files_count(self) -> int:
        return len(self.result.file_infos)

    @property
    def skipped_files(self) -> int:
        return self.result.skipped_files

    @property
    def checkpoint_paths(self) -> list[Path]:
        return self.result.checkpoint_paths

    @threaded
    def _progress_reader(self):
        while True:
            try:
                update = self._progress_queue.get(timeout=0.5)
            except Empty:
                if self.finished or self.cancelled:
                    break
                continue
            name = update.path.name
            if name.endswith(".copy_in_progress"):
                name = name.removesuffix(".copy_in_progress")
            self.current_item = name
            if update.phase == ProgressPhase.VERIFY:
                denom = max(1, update.parallel_verify_readers)
                self.total_done += update.nbytes / denom
            else:
                self.total_done += update.nbytes

    @property
    def percent_done(self) -> int:
        return round(100 / self.todo_size * self.total_done) if self.todo_size else 100

    @property
    def speed(self) -> float:
        now = time.time()
        elapsed = max(now - self._start_time, 1e-6)
        return (self.total_done / 2 if self.verify else self.total_done) / elapsed

    @property
    def progress(self) -> Iterator[str | None]:
        for i in range(1, 101):
            while self.percent_done < i:
                sleep(0.1)
            yield self.current_item

    def run(self):
        self._progress_reader()

        try:
            try:
                self.result = copy_and_seal(
                    source=self.source,
                    destinations=self.destinations,
                    overwrite=self.overwrite,
                    verify=self.verify,
                    skip_existing=self.skip_existing,
                    mhl=self.mhl,
                    legacy_mhl=self.legacy_mhl,
                    cancel_token=self._cancel_token,
                )
            except CopyTreeError as e:
                self.errors = e.args[0]
        finally:
            self.finished = True
            self.total_done = float(self.todo_size)
