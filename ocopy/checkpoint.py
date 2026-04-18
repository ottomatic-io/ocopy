"""Append-only JSONL checkpoint of verified file digests for resumable copy jobs."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
from typing import ClassVar


class Checkpoint:
    """Per-destination copy-root sidecar (``.ocopy-checkpoint``).

    Records one JSON object per line: ``rel_path``, ``size``, ``mtime``, ``xxh64``.
    Append-only with ``fsync`` after each record for crash safety. Readers tolerate
    a truncated final line (partial write).

    Lookups are backed by a process-wide cache keyed by resolved path + file mtime,
    so repeated ``find_hash`` calls on resume don't re-parse the same JSONL file
    once per lookup. The cache is populated lazily and invalidated whenever the
    file is rewritten (record/clear, or any external mtime change).
    """

    FILENAME: ClassVar[str] = ".ocopy-checkpoint"

    # Keyed by the resolved on-disk path. Each entry stores the file's mtime_ns at
    # index time plus the parsed records grouped by (rel_path, size).
    _READ_CACHE: ClassVar[dict[Path, tuple[int, dict[tuple[str, int], list[tuple[float, str]]]]]] = {}

    def __init__(self, copy_root: Path) -> None:
        self._copy_root = copy_root
        self._resolved_path: Path | None = None

    @property
    def path(self) -> Path:
        """Absolute path to the on-disk sidecar; resolved lazily on first access."""
        if self._resolved_path is None:
            self._resolved_path = self._copy_root.resolve() / self.FILENAME
        return self._resolved_path

    def ensure_exists(self) -> None:
        """Create an empty checkpoint file if missing (e.g. cancel before any file).

        Uses ``touch(exist_ok=True)`` since the only invariant needed here is that
        the file exists; durable flushing of an empty marker isn't worth an extra
        open/fsync. Records get their own ``fsync`` via :meth:`record`.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)

    def record(self, rel_path: str, size: int, mtime: float, xxh64: str) -> None:
        """Append one JSONL record and ``fsync`` it for crash safety."""
        payload = json.dumps(
            {"rel_path": rel_path, "size": size, "mtime": mtime, "xxh64": xxh64},
            sort_keys=True,
            separators=(",", ":"),
        )
        data = (payload + "\n").encode("utf-8")
        fd = os.open(str(self.path), os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        self._READ_CACHE.pop(self.path, None)

    def lookup(self, rel_path: str, size: int, mtime: float) -> str | None:
        """Return the recorded ``xxh64`` for a matching ``(rel_path, size, mtime)``.

        Matches on exact size; mtime comparison uses a 2-second tolerance to paper
        over sub-second filesystem truncation (FAT, some network filesystems).
        Last matching record wins so later re-records shadow earlier ones.
        """
        index = self._read_index()
        best: str | None = None
        for rec_mtime, h in index.get((rel_path, size), ()):
            if abs(rec_mtime - mtime) <= 2.0:
                best = h
        return best

    def clear(self) -> None:
        """Delete the checkpoint (called after a successful seal)."""
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()
        self._READ_CACHE.pop(self.path, None)

    def _read_index(self) -> dict[tuple[str, int], list[tuple[float, str]]]:
        """Parse the file into an indexed form, memoized per (path, mtime_ns)."""
        path = self.path
        try:
            st_mtime_ns = path.stat().st_mtime_ns
        except FileNotFoundError:
            return {}

        cached = self._READ_CACHE.get(path)
        if cached is not None and cached[0] == st_mtime_ns:
            return cached[1]

        index: dict[tuple[str, int], list[tuple[float, str]]] = {}
        try:
            raw = path.read_bytes()
        except OSError:
            return {}
        for line in raw.splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            rel = rec.get("rel_path")
            size = rec.get("size")
            mtime = rec.get("mtime")
            h = rec.get("xxh64")
            if not isinstance(rel, str) or not isinstance(h, str) or not h:
                continue
            if not isinstance(size, int) or mtime is None:
                continue
            try:
                mtime_f = float(mtime)
            except (TypeError, ValueError):
                continue
            index.setdefault((rel, size), []).append((mtime_f, h))

        self._READ_CACHE[path] = (st_mtime_ns, index)
        return index
