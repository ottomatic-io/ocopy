"""Tests for ``.ocopy-checkpoint`` JSONL sidecar."""

from ocopy.checkpoint import Checkpoint
from ocopy.ignored import ignored_paths


def test_checkpoint_roundtrip(tmp_path):
    root = tmp_path / "dest_root"
    cp = Checkpoint(root)
    cp.ensure_exists()
    cp.record("foo/bar.txt", 12, 1234.0, "abcd" * 4)
    assert cp.lookup("foo/bar.txt", 12, 1234.0) == "abcd" * 4
    assert cp.lookup("foo/bar.txt", 12, 9999.0) is None


def test_checkpoint_truncated_last_record_ignored(tmp_path):
    """A partially-written final JSON line must be skipped without losing earlier records."""
    root = tmp_path / "r"
    cp = Checkpoint(root)
    cp.ensure_exists()
    cp.record("a.txt", 1, 1.0, "a" * 16)
    cp.record("b.txt", 2, 2.0, "b" * 16)

    # Surgically chop the final record in half, mid-JSON, without a trailing newline.
    raw = cp.path.read_bytes()
    first_nl = raw.index(b"\n")
    cp.path.write_bytes(raw[: first_nl + 1] + b'{"rel_path":"b.txt","siz')

    # The fully-written first record is still recoverable; the truncated one is not.
    assert cp.lookup("a.txt", 1, 1.0) == "a" * 16
    assert cp.lookup("b.txt", 2, 2.0) is None


def test_checkpoint_in_ignored_paths():
    assert ".ocopy-checkpoint" in ignored_paths


def test_checkpoint_clear(tmp_path):
    root = tmp_path / "r"
    cp = Checkpoint(root)
    cp.ensure_exists()
    cp.record("x", 1, 1.0, "b" * 16)
    cp.clear()
    assert not cp.path.exists()


def test_checkpoint_lookup_cache_invalidated_by_record(tmp_path):
    """The lookup cache must reflect new records without stale reads."""
    root = tmp_path / "r"
    cp = Checkpoint(root)
    cp.ensure_exists()
    assert cp.lookup("x", 1, 1.0) is None
    cp.record("x", 1, 1.0, "c" * 16)
    assert cp.lookup("x", 1, 1.0) == "c" * 16
