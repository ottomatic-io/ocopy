import re
from io import BytesIO
from pathlib import Path
from shutil import copystat
from time import sleep
from unittest import mock

import pytest
import requests_mock
from click.testing import CliRunner

from ocopy.cli.ocopy import cli


@pytest.fixture(autouse=True)
def package():
    with mock.patch("ocopy.cli.update.get_version", return_value="0.0.1") as _fixture:
        yield _fixture


@pytest.fixture(autouse=True)
def github():
    with requests_mock.Mocker() as mock_request:
        mock_request.get("https://api.github.com/repos/OTTOMATIC-IO/ocopy/releases/latest", json={"tag_name": "0.6.5"})

        yield mock_request


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, "--help")
    assert result.exit_code == 0


def test_copy(card):
    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 0
    assert "different drives" in result.output
    assert "missing" not in result.output
    assert "in progress" not in result.output


def test_skip(tmp_path, card):
    _, destinations = card

    # Use empty source dir
    src_dir = Path(tmp_path) / "A001"
    src_dir.mkdir()

    src_file = src_dir / "testfile"
    src_file.write_text("some data")

    # Create existing testfile on two of three destinations
    for dst in destinations[-2:]:
        dst_dir = dst / src_dir.name
        dst_dir.mkdir()
        dst_file = dst_dir / "testfile"
        dst_file.write_text("some data")
        copystat(src_file, dst_file)

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 0
    assert "Skipped" in result.output

    for dst in destinations:
        root = dst / src_dir.name
        assert (root / "ascmhl" / "ascmhl_chain.xml").is_file()
        gen = next((root / "ascmhl").glob("*.mhl"))
        assert len(list((root / "ascmhl").glob("*.mhl"))) == 1
        text = gen.read_text(encoding="utf-8")
        assert re.search(r"<xxh64[^>]*>\s*[0-9a-f]+\s*</xxh64>", text, re.IGNORECASE)
        assert not any(p.name == "xxHash.txt" for p in dst.rglob("*"))


def test_legacy_mhl(card):
    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, ["--legacy-mhl", src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 0

    for dst in destinations:
        root = dst / src_dir.name
        assert not (root / "ascmhl").exists()
        flat = list(root.glob("*.mhl"))
        assert len(flat) == 1


def test_legacy_mhl_conflicts_with_no_mhl(card):
    """``--no-mhl --legacy-mhl`` is contradictory and must error out before any copy runs."""
    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, ["--no-mhl", "--legacy-mhl", src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code != 0
    assert "--legacy-mhl cannot be combined with --no-mhl" in result.output
    for dst in destinations:
        assert list(dst.glob("**/*.mhl")) == []
        assert not (dst / src_dir.name / "ascmhl").exists()


def test_no_mhl(card):
    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, ["--no-mhl", src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 0

    for dst in destinations:
        assert list(dst.glob("**/ascmhl/**/*.mhl")) == []
        assert list(dst.glob("**/xxHash.txt")) == []


def test_not_enough_space(card, mocker):
    def fake_free_space(path):
        if "dst_3" in Path(path).as_posix():
            return 0
        return 1024 * 1024 * 1024 * 1024

    mocker.patch("ocopy.cli.ocopy.free_space", fake_free_space)

    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 1
    assert "free space" in result.output


def test_verification_error(card, mocker):
    from contextlib import contextmanager

    class FakeIo:
        def __init__(self, file_path):
            self._file_path = file_path
            self._data = BytesIO(b"some fake data")
            self._damaged_data = BytesIO(b"BROKEN")  # shorter than original to make sure progress update works

        def read(self, count):
            if "dst_3/src/A001XXXX/A001C001_XXXX_XXXX.mov.copy_in_progress" in Path(self._file_path).as_posix():
                return self._damaged_data.read(count)
            return self._data.read(count)

        def write(self, data):
            return len(data)

    @contextmanager
    def fake_open(path, options, **kwds):
        yield FakeIo(path)

    def fake_folder_size(*args):
        return 8 * len("some fake data")

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.utils.folder_size", fake_folder_size)
    mocker.patch("ocopy.verified_copy.copystat", mocker.Mock())
    rename_mock = mocker.patch("pathlib.Path.rename", mocker.Mock())
    unlink_mock = mocker.patch("pathlib.Path.unlink", mocker.Mock())

    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 1
    assert "Failed to copy" in result.output
    assert rename_mock.call_count == 21  # Only good files get renamed
    assert unlink_mock.call_count == 3  # Temp files for the one failed verified_copy


def test_io_error(card, mocker):
    from contextlib import contextmanager

    class FakeIo:
        def __init__(self, file_path):
            self._file_path = file_path
            self._data = BytesIO(b"some fake data")

        def read(self, count):
            return self._data.read(count)

        def write(self, data):
            if "dst_3/src/A001XXXX/A001C001_XXXX_XXXX.mov.copy_in_progress" in Path(self._file_path).as_posix():
                sleep(0.2)
                raise OSError("IO Error")
            return len(data)

    @contextmanager
    def fake_open(path, options, **kwds):
        yield FakeIo(path)

    def fake_folder_size(*args):
        return 8 * len("some fake data")

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.utils.folder_size", fake_folder_size)
    mocker.patch("ocopy.verified_copy.copystat", mocker.Mock())
    rename_mock = mocker.patch("pathlib.Path.rename", mocker.Mock())
    unlink_mock = mocker.patch("pathlib.Path.unlink", mocker.Mock())

    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 1
    assert "Failed to copy" in result.output
    assert rename_mock.call_count == 21  # Only good files get renamed
    assert unlink_mock.call_count == 3  # Temp files for the one failed verified_copy


class _FakeCancelledJob:
    """Fast unit-test double for the CLI-side cancel reporting path."""

    daemon = True

    def __init__(self, source, destinations, *args, **kwargs):
        from ocopy.checkpoint import Checkpoint

        self.finished = True
        self.interrupted_by_cancel = True
        self.verified_files_count = 3
        self.skipped_files = 0
        self.errors = []
        self.speed = 1.0
        self.current_item = None
        self._pct = 0
        self.checkpoint_paths = [Path(d) / Path(source).name / Checkpoint.FILENAME for d in destinations]

    def start(self):
        return None

    def join(self, timeout=None):
        return None

    @property
    def percent_done(self) -> int:
        return min(100, self._pct)

    @property
    def progress(self):
        for _ in range(100):
            self._pct += 1
            yield self.current_item


def test_cancel_human(tmp_path, mocker):
    src = tmp_path / "src"
    src.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()
    mocker.patch("ocopy.cli.ocopy.CopyJob", _FakeCancelledJob)
    runner = CliRunner()
    result = runner.invoke(cli, [src.as_posix(), dst.as_posix()])
    assert result.exit_code == 3
    assert "Cancelled." in result.output
    assert ".ocopy-checkpoint" in result.output


def test_cancel_machine_readable(tmp_path, mocker):
    src = tmp_path / "src"
    src.mkdir()
    dst1 = tmp_path / "dst1"
    dst1.mkdir()
    dst2 = tmp_path / "dst2"
    dst2.mkdir()
    mocker.patch("ocopy.cli.ocopy.CopyJob", _FakeCancelledJob)
    runner = CliRunner()
    result = runner.invoke(cli, ["--machine-readable", src.as_posix(), dst1.as_posix(), dst2.as_posix()])
    assert result.exit_code == 3
    lines = result.output.strip().splitlines()
    summary = lines[-1]
    assert summary.startswith("{")
    assert '"status": "cancelled"' in summary
    # Multi-destination runs surface an array under ``checkpoints``.
    assert '"checkpoints"' in summary
    # Both destination checkpoint paths are reported.
    assert "dst1" in summary
    assert "dst2" in summary


def test_cancel_end_to_end(tmp_path, mocker):
    """A real CopyJob cancelled before start must exit 3 with proper JSON output."""
    from ocopy.verified_copy import CopyJob as RealCopyJob

    src = tmp_path / "src"
    src.mkdir()
    (src / "f.bin").write_bytes(b"hello")
    dst = tmp_path / "dst"
    dst.mkdir()

    def _prestart_cancelled(*args, **kwargs):
        kwargs["auto_start"] = False
        job = RealCopyJob(*args, **kwargs)
        job.cancel()
        job.start()
        return job

    mocker.patch("ocopy.cli.ocopy.CopyJob", _prestart_cancelled)
    runner = CliRunner()
    result = runner.invoke(cli, ["--machine-readable", src.as_posix(), dst.as_posix()])
    assert result.exit_code == 3
    summary = result.output.strip().splitlines()[-1]
    assert '"status": "cancelled"' in summary
    assert '"files_verified": 0' in summary
    assert '"checkpoints"' in summary
    assert (dst / "src" / ".ocopy-checkpoint").is_file()
    assert not (dst / "src" / "ascmhl").exists()


def test_update(card):
    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 0
    assert "update" in result.output
