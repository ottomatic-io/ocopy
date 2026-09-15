"""JSONL event emitter for ``ocopy --machine-readable``.

One JSON object per stdout line. The contract is documented in
``docs/machine-readable.md``; this module is its sole producer.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import click

SCHEMA_VERSION = 1


def _dump(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"))


class JSONLEmitter:
    """Serializes ocopy lifecycle events as JSONL to stdout."""

    def start(
        self,
        *,
        source: Path,
        destinations: list[Path],
        hash_algorithm: str,
        verify: bool,
        skip_existing: bool,
        mhl: bool,
        legacy_mhl: bool,
        contents: bool,
        total_bytes: int,
    ) -> None:
        click.echo(
            _dump(
                {
                    "type": "start",
                    "ts": time.time(),
                    "schema_version": SCHEMA_VERSION,
                    "source": str(source.resolve()),
                    "destinations": [str(d.resolve()) for d in destinations],
                    "hash_algorithm": hash_algorithm,
                    "verify": verify,
                    "skip_existing": skip_existing,
                    "mhl": mhl,
                    "legacy_mhl": legacy_mhl,
                    "contents": contents,
                    "total_bytes": total_bytes,
                }
            )
        )

    def warning(self, code: str, message: str, **extra: Any) -> None:
        payload: dict[str, Any] = {"type": "warning", "ts": time.time(), "code": code, "message": message}
        payload.update(extra)
        click.echo(_dump(payload))

    def progress(
        self,
        *,
        percent: int,
        phase: str,
        current_file: str | None,
        speed_bytes_per_sec: float,
        eta_seconds: float,
    ) -> None:
        click.echo(
            _dump(
                {
                    "type": "progress",
                    "ts": time.time(),
                    "percent": percent,
                    "phase": phase,
                    "current_file": current_file,
                    "speed_bytes_per_sec": speed_bytes_per_sec,
                    "eta_seconds": eta_seconds,
                }
            )
        )

    def result_ok(
        self,
        *,
        files_copied: int,
        files_skipped: int,
        bytes_copied: int,
        duration_s: float,
        speed_bytes_per_sec: float,
        destinations: list[Path],
        hash_algorithm: str,
    ) -> None:
        click.echo(
            _dump(
                {
                    "type": "result",
                    "status": "ok",
                    "ts": time.time(),
                    "files_copied": files_copied,
                    "files_skipped": files_skipped,
                    "bytes_copied": bytes_copied,
                    "duration_s": duration_s,
                    "speed_bytes_per_sec": speed_bytes_per_sec,
                    "destinations": [str(d.resolve()) for d in destinations],
                    "hash_algorithm": hash_algorithm,
                }
            )
        )

    def result_cancelled(self, *, files_verified: int, checkpoints: list[Path]) -> None:
        click.echo(
            _dump(
                {
                    "type": "result",
                    "status": "cancelled",
                    "ts": time.time(),
                    "files_verified": files_verified,
                    "checkpoints": [str(p.resolve()) for p in checkpoints],
                }
            )
        )

    def result_error(
        self,
        *,
        code: str,
        errors: list[dict[str, Any]],
        files_copied: int = 0,
        files_skipped: int = 0,
    ) -> None:
        click.echo(
            _dump(
                {
                    "type": "result",
                    "status": "error",
                    "ts": time.time(),
                    "code": code,
                    "errors": errors,
                    "files_copied": files_copied,
                    "files_skipped": files_skipped,
                }
            )
        )
