"""Skip-existing integrity: honest digests, repair, and fast paths."""

import importlib
from shutil import copystat

import pytest

from ocopy.hash import find_hash, get_hash
from ocopy.verified_copy import CopyTreeError, copy_and_seal


def test_skip_rereads_matching_file_without_manifest(tmp_path, mocker):
    spy = mocker.spy(importlib.import_module("ocopy.hash"), "get_hash")

    src = tmp_path / "src"
    src.mkdir()
    f = src / "hello.bin"
    f.write_bytes(b"hello world")

    d1 = tmp_path / "d1"
    d1.mkdir()
    dst_root = d1 / "src"
    dst_file = dst_root / "hello.bin"
    dst_root.mkdir(parents=True)
    dst_file.write_bytes(b"hello world")
    copystat(f, dst_file)

    copy_and_seal(src, [d1], skip_existing=True)
    # No trusted hash on disk -> pool re-reads the source AND the destination,
    # so at least two ``get_hash`` calls are expected.
    assert spy.call_count >= 2
    h = get_hash(f)
    assert (dst_root / "ascmhl").is_dir()
    assert find_hash(dst_file) == h


def test_skip_destination_bitrot_raises_without_overwrite(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    f = src / "x.bin"
    f.write_bytes(b"1234")

    d1 = tmp_path / "d1"
    d1.mkdir()
    dst_root = d1 / "src"
    dst = dst_root / "x.bin"
    dst_root.mkdir(parents=True)
    dst.write_bytes(b"abcd")
    copystat(f, dst)

    with pytest.raises(CopyTreeError) as exc:
        copy_and_seal(src, [d1], skip_existing=True, overwrite=False)
    assert "Verification failed" in exc.value.args[0][0].error_message


def test_skip_destination_bitrot_repairs_with_overwrite(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    f = src / "x.bin"
    f.write_bytes(b"1234")

    d1 = tmp_path / "d1"
    d1.mkdir()
    dst_root = d1 / "src"
    dst = dst_root / "x.bin"
    dst_root.mkdir(parents=True)
    dst.write_bytes(b"abcd")
    copystat(f, dst)

    copy_and_seal(src, [d1], skip_existing=True, overwrite=True)
    assert dst.read_bytes() == b"1234"
    assert find_hash(dst) == get_hash(f)


def test_no_mhl_dont_verify_skip_zero_extra_reads(tmp_path, mocker):
    spy = mocker.spy(importlib.import_module("ocopy.hash"), "get_hash")

    src = tmp_path / "src"
    src.mkdir()
    f = src / "x.bin"
    f.write_bytes(b"z")

    d1 = tmp_path / "d1"
    d1.mkdir()
    dst_root = d1 / "src"
    dst = dst_root / "x.bin"
    dst_root.mkdir(parents=True)
    dst.write_bytes(b"z")
    copystat(f, dst)

    copy_and_seal(src, [d1], skip_existing=True, mhl=False, verify=False)
    assert spy.call_count == 0
    assert not (dst_root / "ascmhl").exists()


def test_mixed_destinations_single_multi_verify_pool(tmp_path, mocker):
    """One destination exists + needs verify, one missing -> exactly one pool."""
    multi = mocker.spy(importlib.import_module("ocopy.verified_copy"), "multi_xxhash_check")

    src = tmp_path / "src"
    src.mkdir()
    f = src / "only.bin"
    f.write_bytes(b"xyz")

    d1 = tmp_path / "d1"
    d2 = tmp_path / "d2"
    d1.mkdir()
    d2.mkdir()
    r1 = d1 / "src"
    r2 = d2 / "src"
    r1.mkdir(parents=True)
    r2.mkdir(parents=True)
    p1 = r1 / "only.bin"
    p1.write_bytes(b"xyz")
    copystat(f, p1)

    copy_and_seal(src, [d1, d2], skip_existing=True)
    # A single combined verification pool is the whole point of the mixed path.
    assert multi.call_count == 1
