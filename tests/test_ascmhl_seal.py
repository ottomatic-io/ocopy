"""ASC MHL sealing integration and invariants."""

from __future__ import annotations

import random
import re
from pathlib import Path

import pytest
from ascmhl.__version__ import ascmhl_folder_name
from ascmhl.history import MHLHistory

from ocopy.ascmhl_seal import ASCMHLSealError, seal_ascmhl_at_destination, seal_ascmhl_destinations
from ocopy.verified_copy import CopyTreeError, copy_and_seal, copytree

pytest.importorskip("ascmhl")


def _single_mhl(dest_root: Path) -> Path:
    manifests = sorted((dest_root / "ascmhl").glob("*.mhl"))
    assert manifests, f"no generation manifests under {dest_root}"
    return manifests[-1]


def _recorded_xxh64(dest_root: Path, rel_posix: str) -> str | None:
    """Read the xxh64 digest recorded for ``rel_posix`` in the latest generation."""
    history = MHLHistory.load_from_path(str(dest_root))
    media_hash = history.hash_lists[-1].find_media_hash_for_path(rel_posix)
    assert media_hash is not None, f"{rel_posix} missing from manifest"
    entry = media_hash.find_hash_entry_for_format("xxh64")
    return entry.hash_string if entry is not None else None


def test_seal_writes_precomputed_hashes_without_reading_media(tmp_path):
    """Proof-by-corruption that the seal step does not rehash files on disk.

    We copy real content, then truncate the source payload on the destination before
    sealing. If ``seal_ascmhl_destinations`` re-read the file it would hash an empty
    file; because we pass the precomputed xxh64 from ``copytree`` the manifest must
    still contain the *original* digest.
    """
    srcdir = tmp_path / "srcdir"
    srcdir.mkdir()
    payload = random.randbytes(4096)
    (srcdir / "a.bin").write_bytes(payload)

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()
    infos = copytree(srcdir, [dst_parent])

    # Trash the destination payload after the copy/hash step but before sealing.
    (dst_parent / "a.bin").write_bytes(b"")

    seal_ascmhl_destinations([dst_parent], srcdir, infos)

    [original_hash] = [fi.file_hash for fi in infos if fi.source.name == "a.bin"]
    assert _recorded_xxh64(dst_parent, "a.bin") == original_hash


def test_seal_does_not_import_media_hasher_paths(tmp_path, mocker):
    """Regression gate: ``ocopy.ascmhl_seal`` must never call the mhllib entrypoints
    that would open and rehash media files."""
    from ascmhl import commands, hasher

    seal_spy = mocker.spy(commands, "seal_file_path")
    hash_spy = mocker.spy(hasher, "multiple_format_hash_file")

    srcdir = tmp_path / "srcdir"
    srcdir.mkdir()
    (srcdir / "a.bin").write_bytes(random.randbytes(32))

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()
    infos = copytree(srcdir, [dst_parent])
    seal_ascmhl_destinations([dst_parent], srcdir, infos)

    assert seal_spy.call_count == 0
    assert hash_spy.call_count == 0


def test_seal_appends_generation_on_existing_ascmhl_history(tmp_path):
    """A second ``copy_and_seal`` into the same destination must append generation 2,
    not bootstrap a fresh history. ``copy_and_seal`` with ``skip_existing`` avoids a
    real re-copy, exercising ``MHLHistory.load_from_path`` in the seal path."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "one.bin").write_bytes(b"one")

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()

    copy_and_seal(src, [dst_parent])
    dst = dst_parent / "src"
    first_gen = sorted((dst / "ascmhl").glob("*.mhl"))
    assert [p.name.split("_", 1)[0] for p in first_gen] == ["0001"]

    copy_and_seal(src, [dst_parent], skip_existing=True)
    gens = sorted((dst / "ascmhl").glob("*.mhl"))
    assert [p.name.split("_", 1)[0] for p in gens] == ["0001", "0002"]

    history = MHLHistory.load_from_path(str(dst))
    assert len(history.hash_lists) == 2


def test_seal_appends_generation_with_subdirectories(tmp_path):
    """Directory entries recorded in generation 1 must be matched on disk in generation 2,
    otherwise the completeness check reports them missing (camera card layout)."""
    src = tmp_path / "src"
    (src / "DCIM" / "100GOPRO").mkdir(parents=True)
    (src / "DCIM" / "100GOPRO" / "clip.mp4").write_bytes(b"clip")

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()

    copy_and_seal(src, [dst_parent])
    copy_and_seal(src, [dst_parent], skip_existing=True)

    history = MHLHistory.load_from_path(str(dst_parent / "src"))
    assert len(history.hash_lists) == 2


def test_seal_reports_missing_directory_from_history(tmp_path):
    """A directory recorded in history but deleted from the destination must still fail the seal."""
    src = tmp_path / "src"
    (src / "DCIM").mkdir(parents=True)
    (src / "DCIM" / "clip.mp4").write_bytes(b"clip")
    (src / "top.bin").write_bytes(b"top")

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()
    copy_and_seal(src, [dst_parent])

    dst = dst_parent / "src"
    (dst / "DCIM" / "clip.mp4").unlink()
    (dst / "DCIM").rmdir()
    (dst / "top.bin").touch()

    with pytest.raises(ASCMHLSealError, match="completeness check failed"):
        seal_ascmhl_at_destination(dst, dst, [])


def test_seal_rejects_file_info_outside_source_root(tmp_path):
    """``_file_infos_by_relposix`` must reject sources that do not live under ``source_root``
    rather than surfacing an opaque ``ValueError`` from ``Path.relative_to``."""
    from dataclasses import replace

    src = tmp_path / "src"
    src.mkdir()
    (src / "one.bin").write_bytes(b"one")
    stray = tmp_path / "stray.bin"
    stray.write_bytes(b"stray")

    infos = copytree(src, [tmp_path / "dst"])
    infos.append(replace(infos[0], source=stray))

    dst_parent = tmp_path / "dst2"
    dst_parent.mkdir()

    with pytest.raises(ASCMHLSealError, match="outside source_root"):
        seal_ascmhl_destinations([dst_parent], src, infos)


def test_seal_rejects_duplicate_relposix_entries(tmp_path):
    """Two ``FileInfo`` entries collapsing to the same relative path must be surfaced
    loudly rather than silently dropped from the manifest."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "one.bin").write_bytes(b"one")

    infos = copytree(src, [tmp_path / "dst"])
    # Duplicate the single FileInfo to force a relposix collision.
    infos.append(infos[0])

    dst_parent = tmp_path / "dst2"
    dst_parent.mkdir()

    with pytest.raises(ASCMHLSealError, match="duplicate relative path"):
        seal_ascmhl_destinations([dst_parent], src, infos)


def test_seal_error_includes_failing_destination(tmp_path):
    """Failures during multi-destination sealing must name the offending destination."""
    from dataclasses import replace

    src = tmp_path / "src"
    src.mkdir()
    (src / "one.bin").write_bytes(b"one")

    good = tmp_path / "good"
    bad = tmp_path / "bad"
    good.mkdir()
    bad.mkdir()

    infos = copytree(src, [good, bad])
    seal_ascmhl_destinations([good, bad], src, infos)

    tampered = [replace(fi, file_hash="0" * 16) for fi in infos]
    # ``re.escape`` because Windows paths contain backslashes that look like regex
    # escapes (``C:\Users\...`` -> incomplete-escape error in ``re.compile``).
    with pytest.raises(ASCMHLSealError, match=re.escape(str(good))) as excinfo:
        seal_ascmhl_destinations([good, bad], src, tampered)

    # ``good`` is processed first, so it must be the destination surfaced in the error.
    assert "failed sealing destination" in str(excinfo.value)


def test_seal_rejects_mismatched_precomputed_hash(tmp_path):
    """If the injected xxh64 disagrees with a hash already in history, mhllib flags
    the mismatch and we re-raise as ``ASCMHLSealError``."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "one.bin").write_bytes(b"one")

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()
    infos = copytree(src, [dst_parent])
    seal_ascmhl_destinations([dst_parent], src, infos)

    from dataclasses import replace

    tampered = [replace(fi, file_hash="0" * 16) for fi in infos]

    with pytest.raises(ASCMHLSealError):
        seal_ascmhl_destinations([dst_parent], src, tampered)


def test_seal_records_transfer_process_type(tmp_path):
    """ocopy seals the destination after a copy, so the generation's <process> must
    be ``transfer`` — not the ``in-place`` value that describes hashing files in
    situ. Regression guard for the previously-incorrect process type."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.bin").write_bytes(b"x")

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()
    infos = copytree(src, [dst_parent])
    seal_ascmhl_destinations([dst_parent], src, infos)

    history = MHLHistory.load_from_path(str(dst_parent))
    assert history.hash_lists[-1].process_info.process == "transfer"


def test_legacy_mhl_writes_flat_manifest(tmp_path):
    srcdir = tmp_path / "srcdir"
    srcdir.mkdir()
    (srcdir / "a.bin").write_bytes(b"x")

    dst_root = tmp_path / "dst"
    dst_root.mkdir()

    copy_and_seal(srcdir, [dst_root], mhl=True, legacy_mhl=True)

    dest = dst_root / "srcdir"
    flat = list(dest.glob("*.mhl"))
    assert len(flat) == 1
    assert not (dest / "ascmhl").exists()


def test_legacy_mhl_with_non_xxh64_algorithm_raises_in_library_api(tmp_path):
    """The legacy flat MHL v1.1 schema only defines ``<xxhash64be>``. Passing any
    other algorithm to ``copy_and_seal(legacy_mhl=True)`` must surface as a
    ``CopyTreeError`` — this is the library-level guard that backs the CLI's own
    rejection, for programmatic callers (GUI, scripts) that bypass the CLI."""
    srcdir = tmp_path / "srcdir"
    srcdir.mkdir()
    (srcdir / "a.bin").write_bytes(b"x")

    dst_root = tmp_path / "dst"
    dst_root.mkdir()

    with pytest.raises(CopyTreeError) as exc:
        copy_and_seal(srcdir, [dst_root], mhl=True, legacy_mhl=True, algorithm="xxh3")
    assert "xxh64" in exc.value.args[0][0].error_message


def test_seal_against_half_bootstrapped_ascmhl_folder_raises(tmp_path):
    """An ``ascmhl/`` directory without a chain file (e.g. an interrupted bootstrap
    or a manually-created empty folder) must surface as ``ASCMHLSealError`` rather
    than continuing as if no history existed. ``_load_or_bootstrap_history`` sees
    a real dir and tries ``MHLHistory.load_from_path``, which raises
    ``NoMHLChainException``; the seal layer wraps that into ``ASCMHLSealError``."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.bin").write_bytes(b"x")

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()
    infos = copytree(src, [dst_parent])

    # Create an empty ascmhl/ at the destination root — no chain, no manifests.
    (dst_parent / ascmhl_folder_name).mkdir()

    with pytest.raises(ASCMHLSealError, match="missing chain"):
        seal_ascmhl_at_destination(dst_parent, src, infos)
