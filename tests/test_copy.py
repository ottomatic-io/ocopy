import os
from io import BytesIO
from pathlib import Path
from shutil import copystat
from time import sleep

import pytest

from ocopy.hash import find_hash, get_hash
from ocopy.utils import folder_size
from ocopy.verified_copy import (
    CopyJob,
    CopyTreeError,
    VerificationError,
    copy,
    copy_and_seal,
    copytree,
    verified_copy,
)


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
    copystat_mock = mocker.patch("ocopy.verified_copy.copystat", mocker.Mock())
    open_mock = mocker.patch("builtins.open", mocker.mock_open(read_data="test content"))

    src_file = tmpdir / "test-äöüàéè.txt"

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        tmpdir.mkdir(d)

    destinations = [tmpdir / d / "test" for d in destinations]

    copy(src_file, destinations)

    open_mock().write.assert_has_calls(
        [mocker.call("test content"), mocker.call("test content"), mocker.call("test content")]
    )
    assert open_mock().write.call_count == 3
    assert copystat_mock.call_count == 3


def test_copy_error(tmpdir, mocker):
    open_mock = mocker.patch("builtins.open", mocker.mock_open(read_data="test content"))
    open_mock.side_effect = OSError()

    src_file = tmpdir / "test-äöüàéè.txt"

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        tmpdir.mkdir(d)

    destinations = [tmpdir / d / "test" for d in destinations]

    with pytest.raises(OSError):
        copy(src_file, destinations)


def test_verified_copy_skip(tmp_path):
    src_file = tmp_path / "testfile.txt"
    file_size = 1024 * 1024 * 16
    src_file.write_text("x" * file_size)

    destination_dirs = [tmp_path / d / "some" / "sub" / "dir" for d in ["dst_1", "dst_2", "dst_3"]]
    for directory in destination_dirs:
        directory.mkdir(parents=True)

    destinations = [d / "testfile.txt" for d in destination_dirs]

    assert verified_copy(src_file, destinations) == "6878668a929c42c1"
    (tmp_path / "dst_1" / "test.mhl").write_text(
        """<?xml version='1.0' encoding='utf-8'?>
        <hashlist version="1.0">
          <creatorinfo>
            <name>Ben Hagen</name>
            <username>ben</username>
            <hostname>Bens-MacBook-Pro.local</hostname>
            <tool>o/COPY</tool>
            <startdate>2018-01-07T21:31:17Z</startdate>
            <finishdate>2018-01-07T21:31:52Z</finishdate>
          </creatorinfo>
          <hash>
            <file>some/sub/dir/testfile.txt</file>
            <size>7340032000</size>
            <xxhash64be>6878668a929c42c1</xxhash64be>
            <lastmodificationdate>2018-01-05T21:26:59Z</lastmodificationdate>
            <hashdate>2018-01-07T21:31:52Z</hashdate>
          </hash>
        </hashlist>
        """
    )

    assert verified_copy(src_file, destinations, skip_existing=True) == "6878668a929c42c1"


def test_verified_copy_io_error(tmp_path, mocker):
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
                raise OSError()
            return len(data)

    @contextmanager
    def fake_open(path, options, **kwds):
        yield FakeIo(path)

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.verified_copy.copystat", mocker.Mock())
    mocker.patch("pathlib.Path.rename", mocker.Mock())
    unlink_mock = mocker.patch("pathlib.Path.unlink", mocker.Mock())

    src_file = tmp_path / "test-äöüàéè.txt"

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        (tmp_path / d).mkdir()

    destinations = [tmp_path / d / "test" for d in destinations]

    with pytest.raises(OSError):
        verified_copy(src_file, destinations)

    assert unlink_mock.call_count == 3


def test_verified_copy_verification_error(tmp_path, mocker):
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
    mocker.patch("ocopy.verified_copy.copystat", mocker.Mock())
    mocker.patch("pathlib.Path.rename", mocker.Mock())
    unlink_mock = mocker.patch("pathlib.Path.unlink", mocker.Mock())

    src_file = tmp_path / "test-äöüàéè.txt"

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        (tmp_path / d).mkdir()

    destinations = [tmp_path / d / "test" for d in destinations]

    with pytest.raises(VerificationError):
        verified_copy(src_file, destinations)

    assert unlink_mock.call_count == 3


def test_copytree_alphabetical_order(tmp_path):
    """Regression for GH #31: traversal order must be deterministic (sorted by name)."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "zebra.txt").write_text("z")
    (src / "apple.txt").write_text("a")
    folder = src / "folder"
    folder.mkdir()
    (folder / "b.txt").write_text("b")
    (folder / "a.txt").write_text("a")
    (src / "middle.txt").write_text("m")

    dst = tmp_path / "dst" / "src"
    file_infos = copytree(src, [dst])

    rel = [fi.source.relative_to(src).as_posix() for fi in file_infos]
    assert rel == ["apple.txt", "folder/a.txt", "folder/b.txt", "middle.txt", "zebra.txt"]


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

    with pytest.raises(CopyTreeError):
        copytree(src_dir, [destination])

    # Only skip when the modification times match
    os.utime((dst_sub_folders / "existing_file"), (0, 0))
    with pytest.raises(CopyTreeError):
        copytree(src_dir, [destination], skip_existing=True)

    # Make the mtime match
    copystat(src_sub_folders / "existing_file", dst_sub_folders / "existing_file")
    copytree(src_dir, [destination], skip_existing=True)

    # Just overwrite existing files
    copytree(src_dir, [destination], overwrite=True)


def test_copy_and_seal(card):
    from tests.ascmhl_validation import run_ascmhl_debug_verify, validate_ascmhl_xsd

    src_dir, destinations = card

    (src_dir / ".DS_Store").write_text("")
    (src_dir / ".some_hidden_file").write_text("")

    copy_and_seal(src_dir, destinations)

    for dest in destinations:
        root = dest / "src"
        assert not (root / "xxHash.txt").exists()
        assert (root / "ascmhl" / "ascmhl_chain.xml").is_file()
        assert len(list((root / "ascmhl").glob("*.mhl"))) == 1
        validate_ascmhl_xsd(root)
        run_ascmhl_debug_verify(root)

        assert ".DS_Store" not in [e.name for e in dest.glob("**/*")]
        assert ".some_hidden_file" in [e.name for e in dest.glob("**/*")]

        mov = next(root.glob("**/*.mov"))
        assert find_hash(mov) == get_hash(mov)


def test_copy_and_seal_no_mhl(card):
    src_dir, destinations = card

    copy_and_seal(src_dir, destinations, mhl=False)

    for dest in destinations:
        assert list((dest / "src").glob("*.mhl")) == []
        assert not (dest / "src" / "ascmhl").exists()
        assert not (dest / "src" / "xxHash.txt").exists()


def test_copy_job(card):
    src_dir, destinations = card

    job = CopyJob(src_dir, destinations)

    while job.finished is not True:
        sleep(0.1)

    for dest in destinations:
        root = dest / "src"
        assert not (root / "xxHash.txt").exists()
        assert (root / "ascmhl" / "ascmhl_chain.xml").is_file()
        assert len(list((root / "ascmhl").glob("*.mhl"))) == 1


def test_copy_job_cancel(card):
    """Deterministic: cancel before start, then start. No manifest, only checkpoint."""
    src_dir, destinations = card

    job = CopyJob(src_dir, destinations, auto_start=False)
    job.cancel()
    job.start()

    while job.finished is not True:
        sleep(0.05)

    assert job.interrupted_by_cancel
    assert job.verified_files_count == 0
    assert job.result.cancelled is True
    for dest in destinations:
        root = dest / "src"
        assert not (root / "xxHash.txt").exists()
        assert not (root / "ascmhl").exists()
        assert (root / ".ocopy-checkpoint").is_file()
        # Only the checkpoint sidecar should be on disk; no media landed.
        assert sum(1 for p in root.rglob("*") if p.is_file()) == 1


def test_copy_job_cancel_mid_copy(card):
    """Inject a counter-based cancel token so the fire-point is deterministic."""
    src_dir, destinations = card

    ctr = {"n": 0}

    def cancel_mid() -> bool:
        ctr["n"] += 1
        return ctr["n"] > 4

    job = CopyJob(src_dir, destinations, auto_start=False, cancel_token=cancel_mid)
    job.start()

    while job.finished is not True:
        sleep(0.05)

    assert job.interrupted_by_cancel
    assert job.verified_files_count >= 1
    for dest in destinations:
        root = dest / "src"
        assert not (root / "ascmhl").exists()
        assert (root / ".ocopy-checkpoint").is_file()


def test_copy_job_cancel_before_start(card):
    """Lifecycle contract: even a pre-start cancel leaves an empty checkpoint per destination."""
    src_dir, destinations = card

    job = CopyJob(src_dir, destinations, auto_start=False)
    job.cancel()
    job.start()

    while job.finished is not True:
        sleep(0.1)

    assert job.interrupted_by_cancel
    assert job.verified_files_count == 0
    for dest in destinations:
        root = dest / "src"
        assert (root / ".ocopy-checkpoint").is_file()
        assert not (root / "ascmhl").exists()


def test_copy_job_progress(card):
    src_dir, destinations = card

    job = CopyJob(src_dir, destinations)
    assert job.finished is False

    # Make sure to get 100 progress updates
    progress = 0
    for progress, _file_name in enumerate(job.progress, start=1):  # noqa: B007  (used after loop)
        pass

    assert progress == 100


def test_copy_job_verification_error(card, mocker):
    from contextlib import contextmanager

    class FakeIo:
        def __init__(self, file_path):
            self._file_path = file_path
            self._data = BytesIO(b"some fake data")
            self._damaged_data = BytesIO(b"some BROKEN fake data")

        def read(self, count):
            if "dst_3/src/A001XXXX/A001C001_XXXX_XXXX.mov.copy_in_progress" in Path(self._file_path).as_posix():
                return self._damaged_data.read(count)
            return self._data.read(count)

        def write(self, data):
            return len(data)

    @contextmanager
    def fake_open(path, options, **kwds):
        yield FakeIo(path)

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.verified_copy.copystat", mocker.Mock())
    rename_mock = mocker.patch("pathlib.Path.rename", mocker.Mock())
    unlink_mock = mocker.patch("pathlib.Path.unlink", mocker.Mock())

    src_dir, destinations = card

    job = CopyJob(src_dir, destinations)
    assert job.finished is False

    while not job.finished:
        sleep(0.1)

    assert len(job.errors) == 1
    assert "Verification failed" in job.errors[0].error_message
    assert rename_mock.call_count == 21  # Only good files get renamed
    assert unlink_mock.call_count == 3  # Temp files for the one failed verified_copy


def test_copy_job_io_error(card, mocker):
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

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.verified_copy.copystat", mocker.Mock())
    rename_mock = mocker.patch("pathlib.Path.rename", mocker.Mock())
    unlink_mock = mocker.patch("pathlib.Path.unlink", mocker.Mock())

    src_dir, destinations = card

    job = CopyJob(src_dir, destinations)
    assert job.finished is False

    while not job.finished:
        sleep(0.1)

    assert len(job.errors) == 1
    assert "IO Error" in job.errors[0].error_message
    assert rename_mock.call_count == 21  # Only good files get renamed
    assert unlink_mock.call_count == 3  # Temp files for the one failed verified_copy
