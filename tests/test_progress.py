"""Tests for copy/verify progress reporting (``ProgressUpdate`` and ``CopyJob`` reader)."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from time import sleep

import pytest

from ocopy.hash import multi_xxhash_check
from ocopy.progress import ProgressPhase, ProgressUpdate
from ocopy.verified_copy import CopyJob


def _apply_progress_updates(updates: Sequence[ProgressUpdate]) -> float:
    """Same byte accounting as :meth:`ocopy.verified_copy.CopyJob._progress_reader`."""

    total = 0.0
    for update in updates:
        if update.phase == ProgressPhase.VERIFY:
            denom = max(1, update.parallel_verify_readers)
            total += update.nbytes / denom
        else:
            total += update.nbytes
    return total


def _legacy_fractional_verify_total(updates: list[tuple[int, int]]) -> float:
    """Sum of ``nbytes_raw / pool_size`` (legacy per-chunk reporting)."""

    return sum(nbytes_raw / pool_size for nbytes_raw, pool_size in updates)


def test_verify_pool_scaling_matches_legacy_per_chunk_totals():
    """Scaled VERIFY updates must match the old ``nbytes / pool_size`` per-chunk sum."""
    pool_size = 4
    chunks_per_reader = [(100,), (333, 200), (50, 50, 50), (12,)]
    total_legacy = 0.0
    events: list[ProgressUpdate] = []
    for reader_idx, chunks in enumerate(chunks_per_reader):
        path = Path(f"/fake/reader_{reader_idx}")
        for nbytes in chunks:
            total_legacy += nbytes / pool_size
            events.append(
                ProgressUpdate(
                    ProgressPhase.VERIFY,
                    path,
                    nbytes,
                    parallel_verify_readers=pool_size,
                ),
            )

    assert _apply_progress_updates(events) == pytest.approx(total_legacy)
    assert _legacy_fractional_verify_total([(u.nbytes, u.parallel_verify_readers) for u in events]) == pytest.approx(
        total_legacy,
    )


class _HashWorkerThread(Thread):
    """Runs ``multi_xxhash_check`` on this thread so ``get_progress_queue()`` is wired."""

    def __init__(self, paths: list[Path]) -> None:
        super().__init__(daemon=True)
        self._paths = paths
        self._progress_queue: Queue[ProgressUpdate] = Queue()
        self._out_hash: str = ""
        self._exception: BaseException | None = None

    def run(self) -> None:
        try:
            self._out_hash = multi_xxhash_check(self._paths)
        except BaseException as exc:
            self._exception = exc


def test_parallel_identical_files_verify_bar_totals_one_file_size(tmp_path):
    """Many identical files verified in parallel should advance the bar by one file's bytes."""
    n = 5
    size = 12345
    payload = b"z" * size
    paths: list[Path] = []
    for i in range(n):
        p = tmp_path / f"same_{i}.bin"
        p.write_bytes(payload)
        paths.append(p)

    worker = _HashWorkerThread(paths)
    worker.start()
    worker.join(timeout=120.0)
    assert not worker.is_alive(), "multi_xxhash_check should finish within timeout"
    if worker._exception is not None:
        raise worker._exception
    h = worker._out_hash
    q = worker._progress_queue

    assert h != "hashes_do_not_match"

    drained: list[ProgressUpdate] = []
    while True:
        try:
            drained.append(q.get_nowait())
        except Empty:
            break

    assert drained, "expected verify progress events on the capture queue"

    for u in drained:
        assert u.phase == ProgressPhase.VERIFY
        assert u.parallel_verify_readers == n
        assert isinstance(u.nbytes, int)
        assert u.nbytes > 0

    assert _apply_progress_updates(drained) == pytest.approx(float(size))
    assert sum(u.nbytes / n for u in drained) == pytest.approx(float(size))


def test_progress_reader_exits_when_copy_finishes(tmp_path, mocker):
    """Reader must not block forever on an empty queue after the job sets ``finished``."""

    reader_threads: list[Thread] = []
    _RealThread = Thread

    class _CaptureReaderThread(_RealThread):
        def start(self) -> None:
            target = getattr(self, "_target", None)
            if getattr(target, "__qualname__", None) == "CopyJob._progress_reader":
                reader_threads.append(self)
            return super().start()

    # ``@threaded`` closes over ``Thread`` from ``ocopy.utils``; patch there (not ``verified_copy.threaded``).
    mocker.patch("ocopy.utils.Thread", _CaptureReaderThread)

    source = tmp_path / "src"
    source.mkdir()
    (source / "a.txt").write_text("hello")
    dest = tmp_path / "dst"
    dest.mkdir()

    job = CopyJob(source, [dest], overwrite=True)
    while not job.finished:
        sleep(0.05)

    assert len(reader_threads) == 1
    reader_threads[0].join(timeout=5.0)
    assert not reader_threads[0].is_alive(), "progress reader thread should exit after the copy job completes"
