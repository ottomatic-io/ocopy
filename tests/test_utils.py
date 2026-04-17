import platform
import sys
from pathlib import Path
from time import sleep
from types import SimpleNamespace

import ocopy.utils
from ocopy.utils import folder_size, free_space, get_mount, get_user_display_name, threaded


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
