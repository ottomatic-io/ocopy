"""``find_hash`` must not re-parse ASC MHL or legacy MHL for every file (mtime-keyed caches)."""

from __future__ import annotations

import os

import pytest
from lxml.builder import E

from ocopy.ascmhl_seal import seal_ascmhl_at_destination
from ocopy.hash import find_hash, get_hash
from ocopy.mhl import write_mhl_to_destinations
from ocopy.verified_copy import copytree

pytest.importorskip("ascmhl")


def test_ascmhl_load_from_path_once_per_many_files(tmp_path, mocker):
    from ascmhl.history import MHLHistory

    src = tmp_path / "src"
    src.mkdir()
    for i in range(3):
        (src / f"f{i}.txt").write_text(f"x{i}")

    dst = tmp_path / "dst"
    dst.mkdir()
    infos = copytree(src, [dst])
    seal_ascmhl_at_destination(dst, src, infos)

    spy = mocker.spy(MHLHistory, "load_from_path")

    for i in range(3):
        assert find_hash(dst / f"f{i}.txt") == get_hash(dst / f"f{i}.txt")

    assert spy.call_count == 1


def test_ascmhl_cache_invalidates_when_ascmhl_dir_mtime_changes(tmp_path, mocker):
    from ascmhl.history import MHLHistory

    src = tmp_path / "src"
    src.mkdir()
    (src / "x.txt").write_text("hello")

    dst = tmp_path / "dst"
    dst.mkdir()
    infos = copytree(src, [dst])
    seal_ascmhl_at_destination(dst, src, infos)

    spy = mocker.spy(MHLHistory, "load_from_path")

    f = dst / "x.txt"
    find_hash(f)
    assert spy.call_count == 1

    asc_dir = dst / "ascmhl"
    st = asc_dir.stat()
    os.utime(asc_dir, (st.st_atime, st.st_mtime + 1))

    find_hash(f)
    assert spy.call_count == 2


def test_legacy_mhl_index_loaded_once_per_many_files(tmp_path, mocker):
    import ocopy.mhl as mhl_mod

    sub = tmp_path / "sub"
    sub.mkdir()
    for name in ("a.txt", "b.txt", "c.txt"):
        (sub / name).write_text(name)

    # Spy the uncached XML→dict builder: ``@cache`` wraps ``_cached_load_mhl_index``,
    # so spying that name would count every ``find_hash`` call, not cache misses.
    spy = mocker.spy(mhl_mod, "_mhl_text_to_xxh64_index")

    hl = E.hashlist(E.creatorinfo(E.name("t")), version="1.1")
    for name in ("a.txt", "b.txt", "c.txt"):
        p = sub / name
        hl.append(
            E.hash(
                E.file(name),
                E.size(str(p.stat().st_size)),
                E.lastmodificationdate("2018-01-05T21:26:59Z"),
                E.xxhash64be(get_hash(p)),
                E.hashdate("2018-01-07T21:31:52Z"),
            )
        )
    write_mhl_to_destinations(hl, [sub])

    for name in ("a.txt", "b.txt", "c.txt"):
        find_hash(sub / name)

    assert spy.call_count == 1


def test_legacy_mhl_cache_invalidates_when_mhl_mtime_changes(tmp_path, mocker):
    import ocopy.mhl as mhl_mod

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
            E.xxhash64be(get_hash(f)),
            E.hashdate("2018-01-07T21:31:52Z"),
        )
    )
    write_mhl_to_destinations(hl, [sub])

    from ocopy.mhl import find_mhl

    mhl_path = find_mhl(f)
    assert mhl_path is not None

    spy = mocker.spy(mhl_mod, "_mhl_text_to_xxh64_index")

    find_hash(f)
    assert spy.call_count == 1

    st = mhl_path.stat()
    os.utime(mhl_path, (st.st_atime, st.st_mtime + 1))

    find_hash(f)
    assert spy.call_count == 2
