from io import BytesIO
from pathlib import Path
from shutil import copystat
from time import sleep

from click.testing import CliRunner

from ocopy.cli.ocopy import cli


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


def test_skip(tmpdir, card):
    _, destinations = card

    # Use empty source dir
    src_dir = Path(tmpdir) / "A001"
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
        assert len(list(dst.glob("**/*.mhl"))) == 1
        assert len(list(dst.glob("**/*.txt"))) == 1


def test_not_enough_space(card, mocker):
    class MockUsage:
        def __init__(self, path):
            if "dst_3" in Path(path).as_posix():
                self.free = 0
            else:
                self.free = 1024 * 1024 * 1024 * 1024

    mocker.patch("shutil.disk_usage", MockUsage, create=True)

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
        print(f"Open {path}")
        yield FakeIo(path)

    def fake_folder_size(*args):
        return 8 * len("some fake data")

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.utils.folder_size", fake_folder_size)
    mocker.patch("shutil.copystat", mocker.Mock())
    rename_mock = mocker.patch("pathlib.Path.rename", mocker.Mock())
    unlink_mock = mocker.patch("pathlib.Path.unlink", mocker.Mock())

    src_dir, destinations = card

    from importlib import reload
    import ocopy.copy

    reload(ocopy.copy)

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 1
    assert "Failed to copy" in result.output
    assert rename_mock.call_count == 21  # Only good files get renamed
    assert unlink_mock.call_count == 24  # Unlink is tried for all temporary files


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
                raise IOError("IO Error")
            return len(data)

    @contextmanager
    def fake_open(path, options, **kwds):
        print(f"Open {path}")
        yield FakeIo(path)

    def fake_folder_size(*args):
        return 8 * len("some fake data")

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.utils.folder_size", fake_folder_size)
    mocker.patch("shutil.copystat", mocker.Mock())
    rename_mock = mocker.patch("pathlib.Path.rename", mocker.Mock())
    unlink_mock = mocker.patch("pathlib.Path.unlink", mocker.Mock())

    src_dir, destinations = card

    from importlib import reload
    import ocopy.copy

    reload(ocopy.copy)

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 1
    assert "Failed to copy" in result.output
    assert rename_mock.call_count == 21  # Only good files get renamed
    assert unlink_mock.call_count == 24  # Unlink is tried for all temporary files
