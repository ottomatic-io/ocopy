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


@pytest.mark.parametrize("algorithm", ["xxh32", "xxh3", "xxh128", "md5", "sha1", "c4"])
def test_legacy_mhl_rejects_non_xxh64_algorithm(card, algorithm):
    """Legacy MHL v1.1 only defines <xxhash64be>; non-xxh64 must fail before copying."""
    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--legacy-mhl", "--hash-algorithm", algorithm, src_dir.as_posix(), *[d.as_posix() for d in destinations]],
    )
    assert result.exit_code != 0
    assert "--legacy-mhl only supports --hash-algorithm xxh64" in result.output
    for dst in destinations:
        assert list(dst.glob("**/*.mhl")) == []
        assert not (dst / src_dir.name / "ascmhl").exists()


@pytest.mark.parametrize("algorithm", ["md5", "sha1", "xxh32", "xxh64", "xxh3", "xxh128", "c4"])
def test_copy_with_hash_algorithm_writes_matching_ascmhl_element(card, algorithm):
    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--hash-algorithm", algorithm, src_dir.as_posix(), *[d.as_posix() for d in destinations]],
    )
    assert result.exit_code == 0, result.output
    assert "missing" not in result.output
    assert "in progress" not in result.output

    for dst in destinations:
        root = dst / src_dir.name
        assert (root / "ascmhl" / "ascmhl_chain.xml").is_file()
        gen = next((root / "ascmhl").glob("*.mhl"))
        text = gen.read_text(encoding="utf-8")
        # Element name in the manifest matches the chosen algorithm.
        assert re.search(rf"<{algorithm}[^>]*>\s*\S+\s*</{algorithm}>", text), text[:1000]


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

    unlinked_paths: list[Path] = []

    def _capture_unlink(self, missing_ok=False):
        unlinked_paths.append(self)

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.utils.folder_size", fake_folder_size)
    mocker.patch("ocopy.verified_copy.copy_metadata", mocker.Mock())
    rename_mock = mocker.patch("pathlib.Path.rename", mocker.Mock())
    mocker.patch("pathlib.Path.unlink", autospec=True, side_effect=_capture_unlink)

    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 1
    assert "Failed to copy" in result.output
    assert rename_mock.call_count == 21  # Only good files get renamed

    expected_tmps = {dest / "src" / "A001XXXX" / "A001C001_XXXX_XXXX.mov.copy_in_progress" for dest in destinations}
    assert expected_tmps <= set(unlinked_paths)


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

    unlinked_paths: list[Path] = []

    def _capture_unlink(self, missing_ok=False):
        unlinked_paths.append(self)

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.utils.folder_size", fake_folder_size)
    mocker.patch("ocopy.verified_copy.copy_metadata", mocker.Mock())
    rename_mock = mocker.patch("pathlib.Path.rename", mocker.Mock())
    mocker.patch("pathlib.Path.unlink", autospec=True, side_effect=_capture_unlink)

    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 1
    assert "Failed to copy" in result.output
    assert rename_mock.call_count == 21  # Only good files get renamed

    expected_tmps = {dest / "src" / "A001XXXX" / "A001C001_XXXX_XXXX.mov.copy_in_progress" for dest in destinations}
    assert expected_tmps <= set(unlinked_paths)


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
        self.live_speed = 1.0
        self.eta_seconds = 0.0
        self.elapsed = 1.0
        self.current_item = None
        self.current_phase = "copy"
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
    import json

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

    events = [json.loads(line) for line in result.output.strip().splitlines()]
    assert events[0]["type"] == "start"
    assert events[0]["schema_version"] == 1
    final = events[-1]
    assert final["type"] == "result"
    assert final["status"] == "cancelled"
    assert final["files_verified"] == 3
    # Multi-destination runs surface an array under ``checkpoints``.
    assert len(final["checkpoints"]) == 2
    joined = " ".join(final["checkpoints"])
    assert "dst1" in joined
    assert "dst2" in joined


def test_cancel_end_to_end(tmp_path, mocker):
    """A real CopyJob cancelled before start must exit 3 with a result/cancelled event."""
    import json

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
    events = [json.loads(line) for line in result.output.strip().splitlines()]
    final = events[-1]
    assert final["type"] == "result"
    assert final["status"] == "cancelled"
    assert final["files_verified"] == 0
    assert isinstance(final["checkpoints"], list)
    assert (dst / "src" / ".ocopy-checkpoint").is_file()
    assert not (dst / "src" / "ascmhl").exists()


def test_machine_readable_success(card):
    """Real CopyJob on a real card emits a clean start..progress..result/ok stream."""
    import json

    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, ["--machine-readable", src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 0

    lines = result.output.strip().splitlines()
    events = [json.loads(line) for line in lines]  # every line must be valid JSON

    # Schema invariants
    for ev in events:
        assert "type" in ev
        assert "ts" in ev
    assert events[0]["type"] == "start"
    assert events[0]["schema_version"] == 1
    assert events[0]["hash_algorithm"] == "xxh64"
    assert events[0]["verify"] is True
    assert events[0]["total_bytes"] > 0
    assert len(events[0]["destinations"]) == len(destinations)

    # Exactly one terminal result event
    results = [ev for ev in events if ev["type"] == "result"]
    assert len(results) == 1
    assert results[0] is events[-1]
    assert events[-1]["status"] == "ok"
    assert events[-1]["files_copied"] > 0
    assert events[-1]["bytes_copied"] == events[0]["total_bytes"]
    assert events[-1]["speed_bytes_per_sec"] > 0
    assert events[-1]["duration_s"] > 0
    assert events[-1]["hash_algorithm"] == "xxh64"

    # Progress events carry phase + speed + ETA
    progress_events = [ev for ev in events if ev["type"] == "progress"]
    assert progress_events, "expected at least one progress event"
    for ev in progress_events:
        assert ev["phase"] in {"copy", "verify"}
        assert ev["speed_bytes_per_sec"] >= 0
        assert ev["eta_seconds"] >= 0
        assert 0 <= ev["percent"] <= 100


def test_machine_readable_insufficient_space(tmp_path, mocker):
    """Free-space failure emits a single result/error event with code insufficient_space, no start."""
    import json

    src = tmp_path / "src"
    src.mkdir()
    (src / "f.bin").write_bytes(b"x" * 64)
    dst = tmp_path / "dst"
    dst.mkdir()

    mocker.patch("ocopy.cli.ocopy.free_space", return_value=0)

    runner = CliRunner()
    result = runner.invoke(cli, ["--machine-readable", src.as_posix(), dst.as_posix()])
    assert result.exit_code == 1

    events = [json.loads(line) for line in result.output.strip().splitlines()]
    assert not any(ev["type"] == "start" for ev in events)
    assert len(events) == 1
    assert events[0]["type"] == "result"
    assert events[0]["status"] == "error"
    assert events[0]["code"] == "insufficient_space"
    assert events[0]["errors"][0]["available_bytes"] == 0


def test_machine_readable_same_drive_warning(tmp_path, mocker):
    """Two destinations resolving to the same mount produce a same_drive warning between start and result."""
    import json

    src = tmp_path / "src"
    src.mkdir()
    (src / "f.bin").write_bytes(b"hello")
    dst1 = tmp_path / "dst1"
    dst1.mkdir()
    dst2 = tmp_path / "dst2"
    dst2.mkdir()

    mocker.patch("ocopy.cli.ocopy.get_mount", return_value="/")

    runner = CliRunner()
    result = runner.invoke(cli, ["--machine-readable", src.as_posix(), dst1.as_posix(), dst2.as_posix()])
    assert result.exit_code == 0

    events = [json.loads(line) for line in result.output.strip().splitlines()]
    warnings = [ev for ev in events if ev["type"] == "warning" and ev["code"] == "same_drive"]
    assert len(warnings) == 1
    assert events[0]["type"] == "start"
    assert events[-1]["type"] == "result"


def test_machine_readable_error_path(card, mocker):
    """A copy failure surfaces as result/error with the errors array populated."""
    import json
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
    mocker.patch("ocopy.verified_copy.copy_metadata", mocker.Mock())
    mocker.patch("pathlib.Path.rename", mocker.Mock())
    mocker.patch("pathlib.Path.unlink", autospec=True, side_effect=lambda self, missing_ok=False: None)

    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, ["--machine-readable", src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 1

    events = [json.loads(line) for line in result.output.strip().splitlines()]
    assert events[0]["type"] == "start"
    final = events[-1]
    assert final["type"] == "result"
    assert final["status"] == "error"
    assert final["code"] == "copy_failed"
    assert len(final["errors"]) > 0
    assert "source" in final["errors"][0]
    assert "message" in final["errors"][0]


def test_update(card):
    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 0
    assert "update" in result.output
