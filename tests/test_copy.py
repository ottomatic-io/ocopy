import os
from io import BytesIO
from pathlib import Path
from shutil import copystat
from time import sleep

import pytest

from ocopy.copy import copy, copytree, copy_and_seal, CopyJob
from ocopy.hash import get_hash
from ocopy.utils import folder_size


def test_get_hash(tmpdir):
    p = Path(tmpdir) / "test-äöüàéè.txt"

    p.write_text("")
    assert get_hash(p) == "ef46db3751d8e999"

    p.write_text("X" * 1024 * 1024 * 16)
    assert get_hash(p) == "75ba28003b6bfc18"


def test_folder_size(tmpdir):
    p = tmpdir.mkdir("bla") / "test-äöüàéè.txt"
    p.write("asdf" * 8)

    p = tmpdir / "test2-äöüàéè.txt"
    p.write("xxxx" * 4)

    assert folder_size(tmpdir) == 48


def test_copy(tmpdir):
    src_file = tmpdir / "test-äöüàéè.txt"
    file_size = 1024 * 1024 * 16
    src_file.write("x" * file_size)

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        tmpdir.mkdir(d)

    destinations = [tmpdir / d / "test" for d in destinations]

    assert copy(src_file, destinations) == "6878668a929c42c1"
    assert folder_size(tmpdir) == file_size * 4

    for d in destinations:
        d.remove()
    assert folder_size(tmpdir) == file_size


def test_copy_mocked(tmpdir, mocker):
    copystat_mock = mocker.patch("shutil.copystat", mocker.Mock())
    open_mock = mocker.patch("builtins.open", mocker.mock_open(read_data="test content"))

    src_file = tmpdir / "test-äöüàéè.txt"

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        tmpdir.mkdir(d)

    destinations = [tmpdir / d / "test" for d in destinations]

    from importlib import reload
    import ocopy.copy

    reload(ocopy.copy)

    ocopy.copy.copy(src_file, destinations)

    open_mock().write.assert_has_calls(
        [mocker.call("test content"), mocker.call("test content"), mocker.call("test content")]
    )
    assert open_mock().write.call_count == 3
    assert copystat_mock.call_count == 3


def test_copy_error(tmpdir, mocker):
    open_mock = mocker.patch("builtins.open", mocker.mock_open(read_data="test content"))
    open_mock.side_effect = IOError()

    src_file = tmpdir / "test-äöüàéè.txt"

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        tmpdir.mkdir(d)

    destinations = [tmpdir / d / "test" for d in destinations]

    from importlib import reload
    import ocopy.copy

    reload(ocopy.copy)
    with pytest.raises(IOError):
        ocopy.copy.copy(src_file, destinations)


def test_verified_copy_io_error(tmpdir, mocker):
    from contextlib import contextmanager

    class FakeIo:
        def __init__(self, file_path):
            self._file_path = file_path
            self._data = BytesIO(b"some fake data")

        def read(self, count):
            return self._data.read(count)

        def write(self, data):
            if "dst_3" in Path(self._file_path).as_posix():
                sleep(0.2)
                raise IOError()
            return len(data)

    @contextmanager
    def fake_open(path, options, **kwds):
        yield FakeIo(path)

    mocker.patch("builtins.open", fake_open)
    mocker.patch("shutil.copystat", mocker.Mock())
    mocker.patch("pathlib.Path.rename", mocker.Mock())
    unlink_mock = mocker.patch("pathlib.Path.unlink", mocker.Mock())

    src_file = tmpdir / "test-äöüàéè.txt"

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        tmpdir.mkdir(d)

    destinations = [Path(tmpdir) / d / "test" for d in destinations]

    from importlib import reload
    import ocopy.copy

    reload(ocopy.copy)
    with pytest.raises(IOError):
        ocopy.copy.verified_copy(src_file, destinations)

    assert unlink_mock.call_count == 3


def test_verified_copy_verificaton_error(tmpdir, mocker):
    from contextlib import contextmanager

    class FakeIo:
        def __init__(self, file_path):
            self._file_path = file_path
            self._data = BytesIO(b"some fake data")
            self._damaged_data = BytesIO(b"some BROKEN fake data")

        def read(self, count):
            if "dst_3" in Path(self._file_path).as_posix():
                return self._damaged_data.read(count)
            return self._data.read(count)

        def write(self, data):
            return len(data)

    @contextmanager
    def fake_open(path, options, **kwds):
        yield FakeIo(path)

    mocker.patch("builtins.open", fake_open)
    mocker.patch("shutil.copystat", mocker.Mock())
    mocker.patch("pathlib.Path.rename", mocker.Mock())
    unlink_mock = mocker.patch("pathlib.Path.unlink", mocker.Mock())

    src_file = tmpdir / "test-äöüàéè.txt"

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        tmpdir.mkdir(d)

    destinations = [Path(tmpdir) / d / "test" for d in destinations]

    from importlib import reload
    import ocopy.copy

    reload(ocopy.copy)
    with pytest.raises(ocopy.copy.VerificationError):
        ocopy.copy.verified_copy(src_file, destinations)

    assert unlink_mock.call_count == 3


def test_copytree(card):
    src_dir, destinations = card

    file_infos = copytree(src_dir, destinations)

    source_files = [f for f in src_dir.glob("**/*") if f.is_file()]
    assert len(file_infos) == len(source_files)
    assert (
        folder_size(src_dir)
        == folder_size(destinations[0])
        == folder_size(destinations[1])
        == folder_size(destinations[2])
    )

    source_hashes = [get_hash(p) for p in source_files]
    for dest in destinations:
        dest_hashes = [get_hash(p) for p in dest.glob("**/*") if p.is_file()]
        assert source_hashes == dest_hashes

    destination = destinations[0].parent / "dest_x"
    src_folder = src_dir / "XYZ"
    dst_folder = destination / "XYZ"
    src_sub_folders = src_folder / "some" / "sub"
    dst_sub_folders = dst_folder / "some" / "sub"
    src_sub_folders.mkdir(parents=True)
    dst_sub_folders.mkdir(parents=True)

    (src_sub_folders / "existing_file").write_text("foo")
    (dst_sub_folders / "existing_file").write_text("foo")

    # FIXME: Expect FileExistsError
    with pytest.raises(Exception):
        copytree(src_dir, [destination])

    # Only skip when the modification times match
    os.utime((dst_sub_folders / "existing_file"), (0, 0))
    with pytest.raises(Exception):
        copytree(src_dir, [destination], skip_existing=True)

    # Make the mtime match
    copystat(src_sub_folders / "existing_file", dst_sub_folders / "existing_file")
    copytree(src_dir, [destination], skip_existing=True)

    # Just overwrite existing files
    copytree(src_dir, [destination], overwrite=True)


def test_copy_and_seal(card):
    src_dir, destinations = card

    (src_dir / ".DS_Store").write_text("")
    (src_dir / ".some_hidden_file").write_text("")

    copy_and_seal(src_dir, destinations)

    for dest in destinations:
        assert len(list((dest / "src").glob("*.mhl"))) == 1
        assert len((dest / "src" / "xxHash.txt").read_text().splitlines()) == 9
        assert ".DS_Store" not in [e.name for e in dest.glob("**/*")]
        assert ".some_hidden_file" in [e.name for e in dest.glob("**/*")]


def test_copy_job(card):
    src_dir, destinations = card

    job = CopyJob(src_dir, destinations)

    while job.finished is not True:
        sleep(0.1)

    for dest in destinations:
        assert len(list((dest / "src").glob("*.mhl"))) == 1
        assert len((dest / "src" / "xxHash.txt").read_text().splitlines()) == 8


def test_copy_job_cancel(card):
    src_dir, destinations = card

    job = CopyJob(src_dir, destinations)
    job.cancel()

    while job.finished is not True:
        sleep(0.1)

    # Only hash files should be present
    for dest in destinations:
        assert len(list((dest / "src").glob("**/*"))) == 2


def test_copy_job_cancel_before_start(card):
    src_dir, destinations = card

    job = CopyJob(src_dir, destinations, auto_start=False)
    job.cancel()
    job.start()

    while job.finished is not True:
        sleep(0.1)

    # No files should be present
    for dest in destinations:
        assert len(list((dest / "src").glob("**/*"))) == 0


def test_copy_job_progress(card):
    src_dir, destinations = card

    job = CopyJob(src_dir, destinations)
    assert job.finished is False

    # Make sure to get 100 progress updates
    progress = 0
    for progress, file_name in enumerate(job.progress, start=1):
        pass

    assert progress == 100
