import os
import platform
import shutil
import sys
from pathlib import Path
from time import sleep
from types import SimpleNamespace

import ocopy.utils
from ocopy.utils import (
    bytes_to_copy,
    folder_size,
    free_space,
    get_mount,
    get_user_display_name,
    threaded,
)


def test_threaded():
    @threaded
    def forever_running_loop():
        while True:
            sleep(1)

    forever_running_loop()

    # Loop runs in a thread and would block if something was wrong with the threaded wrapper
    assert True


def test_folder_size(tmpdir):
    for i in range(3):
        for j in range(3):
            path = Path(tmpdir) / str(i) / str(j)
            path.mkdir(parents=True)
            for x in range(3):
                (path / str(x)).write_bytes(b"X" * 3)

    assert folder_size(tmpdir) == 3**4


def test_get_user_display_name():
    display_name = get_user_display_name()
    assert isinstance(display_name, str)


def test_get_mount():
    if platform.system() != "Windows":
        assert get_mount(Path("/Some")) == Path("/")
    else:
        assert get_mount(Path("C:/Some")) == Path("C:/")


def test_free_space_returns_positive(tmp_path):
    assert free_space(tmp_path) > 0


def test_free_space_uses_shutil_on_non_darwin(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(ocopy.utils.shutil, "disk_usage", lambda path: SimpleNamespace(free=12345))
    assert free_space(tmp_path) == 12345


def test_free_space_uses_shutil_on_python_313(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(sys, "version_info", (3, 13, 0, "final", 0))
    monkeypatch.setattr(ocopy.utils.shutil, "disk_usage", lambda path: SimpleNamespace(free=54321))
    assert free_space(tmp_path) == 54321


def test_free_space_uses_statfs_on_darwin_py312(monkeypatch, tmp_path):
    if not hasattr(ocopy.utils, "_statfs_bavail_bytes"):
        # _statfs_bavail_bytes only exists on darwin; skip when the module
        # was imported on another platform.
        import pytest

        pytest.skip("darwin-only code path")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(sys, "version_info", (3, 12, 0, "final", 0))
    monkeypatch.setattr(ocopy.utils, "_statfs_bavail_bytes", lambda path: 99)
    assert free_space(tmp_path) == 99


def _make_card(root: Path, count: int = 4, size: int = 100) -> Path:
    src = root / "src"
    src.mkdir()
    for i in range(count):
        f = src / f"f{i}.bin"
        f.write_bytes(bytes([i]) * size)
        os.utime(f, (1_700_000_000 + i, 1_700_000_000 + i))
    return src


def _mirror(src: Path, dst: Path, names: list[str]) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for name in names:
        shutil.copy2(src / name, dst / name)


def test_bytes_to_copy_counts_everything_when_destination_is_empty(tmp_path):
    src = _make_card(tmp_path)
    assert bytes_to_copy(src, tmp_path / "missing") == folder_size(src) == 400
    (tmp_path / "dst").mkdir()
    assert bytes_to_copy(src, tmp_path / "dst") == 400


def test_bytes_to_copy_leaves_out_files_that_are_already_in_place(tmp_path):
    src = _make_card(tmp_path)
    _mirror(src, tmp_path / "dst", ["f0.bin", "f1.bin", "f2.bin"])
    assert bytes_to_copy(src, tmp_path / "dst") == 100


def test_bytes_to_copy_counts_files_that_would_not_be_skipped(tmp_path):
    src = _make_card(tmp_path)
    dst = tmp_path / "dst"
    _mirror(src, dst, ["f0.bin", "f1.bin", "f2.bin"])
    (dst / "f0.bin").write_bytes(b"short")  # same name, different size
    os.utime(dst / "f1.bin", (1_700_000_100, 1_700_000_100))  # same size, mtime off by more than 2 s
    os.utime(dst / "f2.bin", (1_700_000_003, 1_700_000_003))  # same size, mtime within the 2 s tolerance
    assert bytes_to_copy(src, dst) == 300


def test_bytes_to_copy_without_skip_existing_counts_everything(tmp_path):
    src = _make_card(tmp_path)
    _mirror(src, tmp_path / "dst", ["f0.bin", "f1.bin"])
    assert bytes_to_copy(src, tmp_path / "dst", skip_existing=False) == 400


def test_bytes_to_copy_follows_subfolders_and_ignored_files(tmp_path):
    src = _make_card(tmp_path)
    (src / "sub").mkdir()
    (src / "sub" / "deep.bin").write_bytes(b"d" * 50)
    (src / "._f0.bin").write_bytes(b"a" * 10)  # AppleDouble: never copied, never counted
    dst = tmp_path / "dst"
    _mirror(src, dst, ["f0.bin"])
    (dst / "sub").mkdir()
    shutil.copy2(src / "sub" / "deep.bin", dst / "sub" / "deep.bin")
    assert bytes_to_copy(src, dst) == 300
