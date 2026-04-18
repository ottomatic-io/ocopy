from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from queue import Queue
from threading import current_thread


class ProgressPhase(StrEnum):
    COPY = "copy"
    VERIFY = "verify"


@dataclass(frozen=True, slots=True)
class ProgressUpdate:
    """Progress payload. ``nbytes`` is always the raw byte length of the chunk read or written.

    For :attr:`ProgressPhase.VERIFY` updates emitted while multiple files are hashed in
    parallel (see :func:`ocopy.hash.multi_xxhash_check`), ``parallel_verify_readers`` is
    the pool size; the UI consumer divides by it so the bar does not over-count.
    """

    phase: ProgressPhase
    path: Path
    nbytes: int
    parallel_verify_readers: int = 1


def get_progress_queue() -> Queue[ProgressUpdate] | None:
    return getattr(current_thread(), "_progress_queue", None)
