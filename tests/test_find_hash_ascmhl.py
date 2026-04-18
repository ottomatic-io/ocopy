"""``find_hash`` resolution for ASC MHL with legacy fallbacks."""

from __future__ import annotations

import os

import pytest
from lxml.builder import E

from ocopy.ascmhl_seal import seal_ascmhl_at_destination
from ocopy.hash import find_hash, get_hash
from ocopy.mhl import write_mhl_to_destinations
from ocopy.verified_copy import copy_and_seal, copytree, verified_copy

pytest.importorskip("ascmhl")


def test_find_hash_reads_xxh64_from_latest_ascmhl_generation(tmp_path):
    srcdir = tmp_path / "srcdir"
    srcdir.mkdir()
    (srcdir / "x.txt").write_text("hello")

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()
    infos = copytree(srcdir, [dst_parent])
    seal_ascmhl_at_destination(dst_parent, srcdir, infos)

    f = dst_parent / "x.txt"
    assert find_hash(f) == get_hash(f)


def test_find_hash_prefers_ascmhl_over_legacy_mhl(tmp_path):
    srcdir = tmp_path / "srcdir"
    srcdir.mkdir()
    (srcdir / "x.txt").write_text("content-a")

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()
    infos = copytree(srcdir, [dst_parent])
    seal_ascmhl_at_destination(dst_parent, srcdir, infos)

    f = dst_parent / "x.txt"
    hl = E.hashlist(E.creatorinfo(E.name("t")), version="1.1")
    hl.append(
        E.hash(
            E.file("x.txt"),
            E.size("999"),
            E.lastmodificationdate("2018-01-05T21:26:59Z"),
            E.xxhash64be("badbadbadbadbad0"),
            E.hashdate("2018-01-07T21:31:52Z"),
        )
    )
    write_mhl_to_destinations(hl, [dst_parent])

    assert find_hash(f) == get_hash(f)
    assert find_hash(f) != "badbadbadbadbad0"


def test_find_hash_prefers_innermost_nested_ascmhl_history(tmp_path):
    """With ASC MHL histories at both the outer root and a nested subdir, ``find_hash``
    must resolve against the innermost history (plan §D innermost-P rule)."""
    outer = tmp_path / "outer"
    inner = outer / "inner"
    inner.mkdir(parents=True)
    (outer / "top.bin").write_bytes(b"top-data")
    (inner / "deep.bin").write_bytes(b"deep-data")

    outer_dst = tmp_path / "outer_dst"
    outer_dst.mkdir()
    infos = copytree(outer, [outer_dst])
    seal_ascmhl_at_destination(outer_dst, outer, infos)

    # Nest a second, independent ASC MHL history that only covers ``inner/``.
    inner_dst = outer_dst / "inner"
    inner_infos = [fi for fi in infos if fi.source.is_relative_to(inner)]
    seal_ascmhl_at_destination(inner_dst, inner, inner_infos)

    assert (outer_dst / "ascmhl").is_dir()
    assert (inner_dst / "ascmhl").is_dir()

    # ``deep.bin`` is covered by both histories; innermost (``inner_dst/ascmhl``) wins.
    deep = inner_dst / "deep.bin"
    top = outer_dst / "top.bin"
    assert find_hash(deep) == get_hash(deep)
    assert find_hash(top) == get_hash(top)


def test_verified_copy_skip_uses_dest_ascmhl(tmp_path):
    """Skip-existing path must return the hash recorded in the destination ASC MHL
    without re-hashing the source (plan §E)."""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    payload = b"original-bytes-0123456789"
    (src_dir / "file.bin").write_bytes(payload)

    dst_parent = tmp_path / "dst"
    dst_parent.mkdir()
    copy_and_seal(src_dir, [dst_parent])

    src = src_dir / "file.bin"
    dst = dst_parent / "src" / "file.bin"
    expected = get_hash(src)

    # Tamper with the source in place (same size and mtime) so any stray source
    # re-hash would disagree with the recorded digest.
    mtime = src.stat().st_mtime
    src.write_bytes(b"T" * len(payload))
    os.utime(src, (mtime, mtime))

    assert verified_copy(src, [dst], skip_existing=True) == expected


def test_find_hash_falls_back_to_legacy_flat_mhl(tmp_path):
    """Without any ASC MHL history, ``find_hash`` must fall back to a legacy flat ``.mhl``."""
    sub = tmp_path / "sub"
    sub.mkdir()
    f = sub / "f.txt"
    f.write_text("hello")

    hl = E.hashlist(E.creatorinfo(E.name("t")), version="1.1")
    hl.append(
        E.hash(
            E.file("f.txt"),
            E.size(str(f.stat().st_size)),
            E.lastmodificationdate("2018-01-05T21:26:59Z"),
            E.xxhash64be("deadbeef" * 2),
            E.hashdate("2018-01-07T21:31:52Z"),
        )
    )
    write_mhl_to_destinations(hl, [sub])

    assert find_hash(f) == "deadbeef" * 2
