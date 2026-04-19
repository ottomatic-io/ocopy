import os
import threading
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


def _install_counting_rename_tmps(mocker):
    """Count tmp→final commits by wrapping :func:`ocopy.verified_copy._rename_tmps`.

    Tests often mock ``pathlib.Path.rename``; counting those calls picks up every
    thread. Summing ``len(tmps)`` per wrapper call matches the number of
    successful commits for this pipeline only.
    """
    import ocopy.verified_copy as vc

    real_rename_tmps = vc._rename_tmps
    counter: dict[str, int] = {"n": 0}

    def wrapped(tmps, final_paths):
        counter["n"] += len(tmps)
        return real_rename_tmps(tmps, final_paths)

    mocker.patch("ocopy.verified_copy._rename_tmps", wrapped)
    return counter


def _is_dst3_a001c001_verify_tmp(file_path) -> bool:
    """Match the poisoned tmp path across platforms (avoids brittle substring tests on Windows)."""
    p = Path(file_path)
    return p.name == "A001C001_XXXX_XXXX.mov.copy_in_progress" and "dst_3" in p.parts and "A001XXXX" in p.parts


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
    class FakeIo:
        def __init__(self, file_path):
            self._file_path = file_path
            self._data = BytesIO(b"some fake data")

        def read(self, count):
            return self._data.read(count)

        def write(self, data):
            if "dst_3" in Path(self._file_path).parts:
                sleep(0.2)
                raise OSError()
            return len(data)

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

        def close(self):
            pass

    def fake_open(path, mode="r", *args, **kwargs):
        return FakeIo(path)

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
    class FakeIo:
        def __init__(self, file_path):
            self._file_path = file_path
            self._data = BytesIO(b"some fake data")
            self._damaged_data = BytesIO(b"some BROKEN fake data")

        def read(self, count):
            if "dst_3" in Path(self._file_path).parts:
                return self._damaged_data.read(count)
            return self._data.read(count)

        def write(self, data):
            return len(data)

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

        def close(self):
            pass

    def fake_open(path, mode="r", *args, **kwargs):
        return FakeIo(path)

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
    """Traversal order must be deterministic (lexicographic by basename, depth-first)."""
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


def test_copy_job_finishes_while_source_tree_grows(tmp_path):
    """CopyJob must reach ``finished`` when the source tree grows during the run.

    Two background writers add files while copy is in progress: one under a
    subdirectory that has not been visited yet, and one at the source root
    (the directory ``copytree`` is walking). Writers run until the job finishes
    or the test tears down, so the scenario does not depend on a fixed write
    count.

    Assertions require that some files appeared after the job started, so the
    test is not a no-op on unrealistically fast hardware.
    """
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    (src / "a_big.bin").write_bytes(b"x" * (32 * 1024 * 1024))
    late = src / "z_sub"
    late.mkdir()
    (late / "seed.txt").write_text("seed")

    stop = threading.Event()
    added_after_start = {"sub": 0, "root": 0}
    job_started = threading.Event()

    def spam_sub() -> None:
        i = 0
        while not stop.is_set():
            (late / f"sub_{i}.txt").write_text("x")
            i += 1
            if job_started.is_set():
                added_after_start["sub"] += 1
            # Without a small yield, slow CI can accumulate a huge z_sub snapshot
            # (one verified_copy per file) and hit the join timeout even though the
            # job is making progress — not a hang.
            sleep(0.008)

    def spam_root() -> None:
        i = 0
        while not stop.is_set():
            # Prefix 'zz_' so these always sort after 'z_sub' and cannot be missed
            # for trivial "scanned first" reasons.
            (src / f"zz_root_{i}.txt").write_text("x")
            i += 1
            if job_started.is_set():
                added_after_start["root"] += 1
            sleep(0.008)

    writers = [threading.Thread(target=spam_sub), threading.Thread(target=spam_root)]
    for w in writers:
        w.start()
    job: CopyJob | None = None
    try:
        job = CopyJob(src, [dst], mhl=False, verify=True)
        job_started.set()
        # Generous wall clock: this test is bounded by CI I/O, not product logic.
        job.join(timeout=300)
        assert job.finished, "CopyJob did not finish within 300s (possible hang while copying a growing tree)"
        assert not job.errors, f"CopyJob reported errors: {[e.error_message for e in job.errors]}"
        assert (dst / "src" / "z_sub" / "seed.txt").is_file()
        # The race must have actually happened in at least one spammer; otherwise
        # this test is not exercising issue #8 and should fail loudly.
        assert added_after_start["sub"] + added_after_start["root"] > 0, (
            "no files were added after the job started; test did not exercise concurrent source growth"
        )
    finally:
        stop.set()
        for w in writers:
            w.join(timeout=2)
        # If join(timeout) returned early, stop the copy thread so later tests are not
        # affected (CopyJob is a daemon but still consumes I/O and can disturb mocks).
        if job is not None and job.is_alive():
            job.cancel()
            job.join(timeout=60)


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
    class FakeIo:
        def __init__(self, file_path):
            self._file_path = file_path
            self._data = BytesIO(b"some fake data")
            self._damaged_data = BytesIO(b"some BROKEN fake data")

        def read(self, count):
            if _is_dst3_a001c001_verify_tmp(self._file_path):
                return self._damaged_data.read(count)
            return self._data.read(count)

        def write(self, data):
            return len(data)

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

        def close(self):
            pass

    def fake_open(path, mode="r", *args, **kwargs):
        # Return a real file-like object, not a @contextmanager wrapper: mhllib and
        # ``with open(...)`` both expect ``open()`` to yield something with read/write.
        return FakeIo(path)

    unlinked_paths: list[Path] = []

    def _capture_unlink(self, missing_ok=False):
        unlinked_paths.append(self)

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.verified_copy.copystat", mocker.Mock())
    mocker.patch("pathlib.Path.rename", mocker.Mock())
    rename_count = _install_counting_rename_tmps(mocker)
    mocker.patch("pathlib.Path.unlink", autospec=True, side_effect=_capture_unlink)

    src_dir, destinations = card
    media_file_count = sum(1 for p in src_dir.rglob("*.mov") if p.is_file())
    expected_rename_commits = (media_file_count - 1) * len(destinations)

    job = CopyJob(src_dir, destinations)
    assert job.finished is False

    while not job.finished:
        sleep(0.1)

    assert len(job.errors) == 1
    assert "Verification failed" in job.errors[0].error_message
    assert rename_count["n"] == expected_rename_commits

    # Assert semantically that the failing file's tmps were cleaned up on every
    # destination.
    expected_tmps = {dest / "src" / "A001XXXX" / "A001C001_XXXX_XXXX.mov.copy_in_progress" for dest in destinations}
    assert expected_tmps <= set(unlinked_paths)


def test_copy_job_io_error(card, mocker):
    class FakeIo:
        def __init__(self, file_path):
            self._file_path = file_path
            self._data = BytesIO(b"some fake data")

        def read(self, count):
            return self._data.read(count)

        def write(self, data):
            if _is_dst3_a001c001_verify_tmp(self._file_path):
                sleep(0.2)
                raise OSError("IO Error")
            return len(data)

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

        def close(self):
            pass

    def fake_open(path, mode="r", *args, **kwargs):
        return FakeIo(path)

    unlinked_paths: list[Path] = []

    def _capture_unlink(self, missing_ok=False):
        unlinked_paths.append(self)

    mocker.patch("builtins.open", fake_open)
    mocker.patch("ocopy.verified_copy.copystat", mocker.Mock())
    mocker.patch("pathlib.Path.rename", mocker.Mock())
    rename_count = _install_counting_rename_tmps(mocker)
    mocker.patch("pathlib.Path.unlink", autospec=True, side_effect=_capture_unlink)

    src_dir, destinations = card
    media_file_count = sum(1 for p in src_dir.rglob("*.mov") if p.is_file())
    expected_rename_commits = (media_file_count - 1) * len(destinations)

    job = CopyJob(src_dir, destinations)
    assert job.finished is False

    while not job.finished:
        sleep(0.1)

    assert len(job.errors) == 1
    assert "IO Error" in job.errors[0].error_message
    assert rename_count["n"] == expected_rename_commits

    # Assert semantically that the failing file's tmps were cleaned up on every
    # destination.
    expected_tmps = {dest / "src" / "A001XXXX" / "A001C001_XXXX_XXXX.mov.copy_in_progress" for dest in destinations}
    assert expected_tmps <= set(unlinked_paths)
