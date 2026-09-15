import contextlib
import os
import stat
import threading
from io import BytesIO
from pathlib import Path
from shutil import copystat
from time import sleep

import pytest

from ocopy.hash import SUPPORTED_ALGORITHMS, _new_hasher, find_hash, get_hash
from ocopy.utils import folder_size
from ocopy.verified_copy import (
    CopyJob,
    CopyTreeError,
    VerificationError,
    copy,
    copy_and_seal,
    copy_metadata,
    copytree,
    verified_copy,
)

# ``os.chflags`` and ``st_flags`` are BSD/macOS only. Reach them indirectly so the
# type checker, which runs against Linux in CI, does not flag them as missing.
_chflags = getattr(os, "chflags", None)
_needs_flags = pytest.mark.skipif(_chflags is None, reason="requires BSD/macOS file flags")


def _set_flags(path, flags):
    assert _chflags is not None
    _chflags(path, flags)


def _get_flags(path) -> int:
    return getattr(os.stat(path), "st_flags", 0)


def _unlock_tree(root):
    """Clear flags on everything under ``root``.

    pytest cannot remove a locked file, and one left behind makes every later
    session fail at basetemp setup, so this has to cover files a failing assertion
    never got to name.
    """
    if _chflags is None:
        return
    for p in root.rglob("*"):
        with contextlib.suppress(OSError):
            _chflags(p, 0)


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


_HASH_VECTORS_EMPTY = {
    "md5": "d41d8cd98f00b204e9800998ecf8427e",
    "sha1": "da39a3ee5e6b4b0d3255bfef95601890afd80709",
    "xxh32": "02cc5d05",
    "xxh64": "ef46db3751d8e999",
    "xxh3": "2d06800538d394c2",
    "xxh128": "99aa06d3014798d86001c324468d497f",
    "c4": "c459dsjfscH38cYeXXYogktxf4Cd9ibshE3BHUo6a58hBXmRQdZrAkZzsWcbWtDg5oQstpDuni4Hirj75GEmTc1sFT",
}
_HASH_VECTORS_16MB_OF_X = {
    "md5": "3cafcbe66fcaf4f8d5174a763b7cf974",
    "sha1": "94d3e62de071d61513dea8135995dcce57213b35",
    "xxh32": "649ffba7",
    "xxh64": "75ba28003b6bfc18",
    "xxh3": "761e87524f56c22d",
    "xxh128": "f12a2d9fe5ea3de9761e87524f56c22d",
    "c4": "c45z2pJEDXuECUXBNruZU7q4vDERZsGa2Gx7VHxQYZjqt2dEiF1FkxUxxJpqJkNZZLFQMnkUkrAVjhCR9JrGSupE1G",
}


@pytest.mark.parametrize("algorithm", list(_HASH_VECTORS_EMPTY))
def test_get_hash(tmpdir, algorithm):
    p = Path(tmpdir) / "test-äöüàéè.txt"

    p.write_text("")
    assert get_hash(p, algorithm=algorithm) == _HASH_VECTORS_EMPTY[algorithm]

    p.write_text("X" * 1024 * 1024 * 16)
    assert get_hash(p, algorithm=algorithm) == _HASH_VECTORS_16MB_OF_X[algorithm]


def test_new_hasher_rejects_unsupported_algorithm():
    """``_new_hasher`` is the single chokepoint for algorithm selection; an unknown
    name must surface as ``ValueError`` (with the algorithm in the message) rather
    than fall back silently. Locks in the contract that ``SUPPORTED_ALGORITHMS``
    is the authoritative whitelist."""
    with pytest.raises(ValueError, match="unsupported hash algorithm"):
        _new_hasher("not-a-real-algo")
    # Sanity: every advertised algorithm constructs.
    for name in SUPPORTED_ALGORITHMS:
        _new_hasher(name)


def test_folder_size(tmpdir):
    p = tmpdir.mkdir("bla") / "test-äöüàéè.txt"
    p.write("asdf" * 8)

    p = tmpdir / "test2-äöüàéè.txt"
    p.write("xxxx" * 4)

    assert folder_size(tmpdir) == 48


_COPY_VECTORS_16MB_OF_X_LOWER = {
    "md5": "d4760a6c6500b8c7fbb09e4c65bc558a",
    "sha1": "f78e872d42c1a6c50c12b410b1bd2b79fbf14653",
    "xxh32": "9044eda0",
    "xxh64": "6878668a929c42c1",
    "xxh3": "cd8aadbe2f17be1d",
    "xxh128": "fd5e5fbd82c7d930cd8aadbe2f17be1d",
    "c4": "c44VXhZazyVfyY282ejaDJAUoPnd8GYrcDCzYrjvKixrsRTQHaMbcBUtqdM22hYnWUtcf6UbjcKcVoy96CnGTom4XD",
}


@pytest.mark.parametrize("algorithm", list(_COPY_VECTORS_16MB_OF_X_LOWER))
def test_copy(tmpdir, algorithm):
    src_file = tmpdir / "test-äöüàéè.txt"
    file_size = 1024 * 1024 * 16
    src_file.write("x" * file_size)

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        tmpdir.mkdir(d)

    destinations = [tmpdir / d / "test" for d in destinations]

    assert copy(src_file, destinations, algorithm=algorithm) == _COPY_VECTORS_16MB_OF_X_LOWER[algorithm]
    assert folder_size(tmpdir) == file_size * 4

    for d in destinations:
        d.remove()
    assert folder_size(tmpdir) == file_size


def test_copy_mocked(tmpdir, mocker):
    copy_metadata_mock = mocker.patch("ocopy.verified_copy.copy_metadata", mocker.Mock())
    open_mock = mocker.patch("builtins.open", mocker.mock_open(read_data=b"test content"))

    src_file = tmpdir / "test-äöüàéè.txt"

    destinations = ["dst_1", "dst_2", "dst_3"]
    for d in destinations:
        tmpdir.mkdir(d)

    destinations = [tmpdir / d / "test" for d in destinations]

    copy(src_file, destinations)

    open_mock().write.assert_has_calls(
        [mocker.call(b"test content"), mocker.call(b"test content"), mocker.call(b"test content")]
    )
    assert open_mock().write.call_count == 3
    assert copy_metadata_mock.call_count == 3


def test_copy_error(tmpdir, mocker):
    open_mock = mocker.patch("builtins.open", mocker.mock_open(read_data=b"test content"))
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
    mocker.patch("ocopy.verified_copy.copy_metadata", mocker.Mock())
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
    mocker.patch("ocopy.verified_copy.copy_metadata", mocker.Mock())
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


def test_copy_job_finishes_while_source_tree_grows(tmp_path, mocker):
    """CopyJob must reach ``finished`` when the source tree grows during the run.

    Two background writers add files while copy is in progress: one under a
    subdirectory that has not been visited yet, and one at the source root
    (the directory ``copytree`` is walking). Writers run until the job finishes
    or the test tears down, so the scenario does not depend on a fixed write
    count.

    Synchronize on the first real ``copy()`` call rather than a wall-clock
    "job started" guess: the big root file blocks until both writers have added
    at least one file after copy has actually begun. This keeps the test
    meaningful on very fast CI while still exercising issue #8.
    """
    import ocopy.verified_copy as vc

    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    big_file = src / "a_big.bin"
    big_file.write_bytes(b"x" * (32 * 1024 * 1024))
    late = src / "z_sub"
    late.mkdir()
    (late / "seed.txt").write_text("seed")

    stop = threading.Event()
    copy_started = threading.Event()
    sub_added_after_copy_started = threading.Event()
    root_added_after_copy_started = threading.Event()
    growth_observed = {"ok": False}

    def _mark_added_after_copy_started(which: str) -> None:
        if not copy_started.is_set():
            return
        if which == "sub":
            sub_added_after_copy_started.set()
        else:
            root_added_after_copy_started.set()

    def spam_sub() -> None:
        i = 0
        while not stop.is_set():
            (late / f"sub_{i}.txt").write_text("x")
            i += 1
            _mark_added_after_copy_started("sub")
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
            _mark_added_after_copy_started("root")
            sleep(0.008)

    real_copy = vc.copy

    def gated_copy(src_file, destinations, chunk_size=1024 * 1024, algorithm="xxh64"):
        if src_file == big_file:
            copy_started.set()
            growth_observed["ok"] = sub_added_after_copy_started.wait(timeout=5) and root_added_after_copy_started.wait(
                timeout=5
            )
        return real_copy(src_file, destinations, chunk_size, algorithm=algorithm)

    mocker.patch("ocopy.verified_copy.copy", side_effect=gated_copy)

    writers = [threading.Thread(target=spam_sub), threading.Thread(target=spam_root)]
    for w in writers:
        w.start()
    job: CopyJob | None = None
    try:
        job = CopyJob(src, [dst], mhl=False, verify=True)
        # Generous wall clock: this test is bounded by CI I/O, not product logic.
        job.join(timeout=300)
        assert job.finished, "CopyJob did not finish within 300s (possible hang while copying a growing tree)"
        assert not job.errors, f"CopyJob reported errors: {[e.error_message for e in job.errors]}"
        assert (dst / "src" / "z_sub" / "seed.txt").is_file()
        # The race must have actually happened in both writer threads; otherwise
        # this test is not exercising issue #8 and should fail loudly.
        assert growth_observed["ok"], (
            "writers did not add files after copy actually started; test did not exercise concurrent source growth"
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
    mocker.patch("ocopy.verified_copy.copy_metadata", mocker.Mock())
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
    mocker.patch("ocopy.verified_copy.copy_metadata", mocker.Mock())
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


@_needs_flags
def test_locked_source_file_is_copied_unlocked(tmp_path):
    """A ``uchg`` source must copy, and the delivered file must not be locked.

    ``copystat`` copies ``st_flags``. Applying it to the ``.copy_in_progress``
    temp made the temp itself immutable, so the rename failed with ``EPERM`` and
    took the whole job down. GoPro cards ship such files
    (``Get_started_with_GoPro.url``), so every card job failed. The lock is not
    replicated at all; the timestamps still are, so a later ``skip_existing`` run
    can fast-skip the file.
    """
    src = tmp_path / "src"
    src.mkdir()
    locked = src / "Get_started_with_GoPro.url"
    locked.write_text("[InternetShortcut]\n")
    old_mtime = 1_000_000_000
    os.utime(locked, (old_mtime, old_mtime))
    _set_flags(locked, stat.UF_IMMUTABLE)
    plain = src / "clip.mp4"
    plain.write_text("x" * 1024)

    dst = tmp_path / "dst"
    dst.mkdir()
    copied = dst / locked.name
    try:
        copytree(src, [dst])

        assert copied.read_text() == "[InternetShortcut]\n"
        assert not _get_flags(copied) & stat.UF_IMMUTABLE
        assert abs(os.stat(copied).st_mtime - old_mtime) <= 2
        assert not list(dst.rglob("*.copy_in_progress"))
        assert (dst / plain.name).exists()

        copytree(src, [dst], skip_existing=True)
    finally:
        _unlock_tree(tmp_path)


@_needs_flags
def test_copy_metadata_drops_locking_flags_and_keeps_the_rest(tmp_path):
    src = tmp_path / "src.bin"
    src.write_text("src")
    dst = tmp_path / "dst.bin"
    dst.write_text("dst")
    os.chmod(src, 0o640)
    old_mtime = 1_000_000_000
    os.utime(src, (old_mtime, old_mtime))
    _set_flags(src, stat.UF_IMMUTABLE | stat.UF_APPEND | stat.UF_NOUNLINK | stat.UF_HIDDEN)
    try:
        copy_metadata(src, dst)

        flags = _get_flags(dst)
        assert not flags & (stat.UF_IMMUTABLE | stat.UF_APPEND | stat.UF_NOUNLINK)
        assert flags & stat.UF_HIDDEN
        assert stat.S_IMODE(os.stat(dst).st_mode) == 0o640
        assert os.stat(dst).st_mtime == old_mtime
    finally:
        _unlock_tree(tmp_path)


def test_copy_metadata_is_copystat_without_file_flags(tmp_path, mocker):
    """Where the platform has no file flags, metadata replication is ``shutil.copystat`` itself."""
    src = tmp_path / "src.bin"
    src.write_text("src")
    dst = tmp_path / "dst.bin"
    dst.write_text("dst")
    no_flags_os = mocker.Mock(wraps=os, spec=[name for name in dir(os) if name != "chflags"])
    mocker.patch("ocopy.verified_copy.os", no_flags_os)
    copystat_mock = mocker.patch("ocopy.verified_copy.copystat")

    copy_metadata(src, dst)

    copystat_mock.assert_called_once_with(src, dst)


@_needs_flags
def test_overwrite_does_not_replace_a_destination_someone_locked(tmp_path):
    """ocopy never creates a locked destination, so a lock there is a human's decision."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "clip.mp4").write_text("new")

    dst = tmp_path / "dst"
    dst.mkdir()
    existing = dst / "clip.mp4"
    existing.write_text("keep me")
    _set_flags(existing, stat.UF_IMMUTABLE)
    try:
        with pytest.raises(CopyTreeError):
            copytree(src, [dst], overwrite=True)

        assert existing.read_text() == "keep me"
        assert _get_flags(existing) & stat.UF_IMMUTABLE
    finally:
        _unlock_tree(tmp_path)


_SF_ARCHIVED = 0x00010000  # ``stat.SF_ARCHIVED``; spelled out because Linux lacks it


def _simulate_exfat_archive_flags(
    mocker, archived: set[Path], extra_flags: int = 0, archive_flag: int = _SF_ARCHIVED
) -> list[tuple[str, int]]:
    """Make ``archived`` report ``SF_ARCHIVED`` and ``chflags`` refuse system flags.

    That is what macOS shows for files on an exFAT card, and what a non-root
    ``chflags`` does with the flag. A normal user cannot set ``SF_ARCHIVED`` for
    real, so both sides are simulated. Returns the ``chflags`` calls that went through.
    """
    real_stat = os.stat
    applied: list[tuple[str, int]] = []

    def fake_stat(path, *args, **kwargs):
        st = real_stat(path, *args, **kwargs)
        if Path(path) not in archived:
            return st
        fields = {name: getattr(st, name) for name in dir(st) if name.startswith("st_")}
        fields["st_flags"] = archive_flag | extra_flags
        return type("FakeStat", (), fields)()

    def fake_chflags(path, flags, *args, **kwargs):
        if flags & 0xFFFF0000:
            raise PermissionError(1, "Operation not permitted", str(path))
        applied.append((os.fspath(path), flags))

    mocker.patch.object(os, "stat", side_effect=fake_stat)
    mocker.patch.object(os, "chflags", side_effect=fake_chflags, create=True)
    return applied


def test_copy_metadata_drops_system_flags_and_keeps_the_rest(tmp_path, mocker):
    """An ``SF_ARCHIVED`` source (any file on an exFAT card) must not fail with ``EPERM``.

    ``copystat`` tried to set the flag on the destination, which only root may
    do, so every such file failed to copy. Times, mode and the user flags ocopy
    keeps must still arrive.
    """
    src = tmp_path / "mdb_h_v01.bk"
    src.write_text("src")
    dst = tmp_path / "dst.bin"
    dst.write_text("dst")
    os.chmod(src, 0o640)
    old_mtime = 1_000_000_000
    os.utime(src, (old_mtime, old_mtime))
    hidden = getattr(stat, "UF_HIDDEN", 0x8000)
    immutable = getattr(stat, "UF_IMMUTABLE", 0x2)
    applied = _simulate_exfat_archive_flags(mocker, {src}, extra_flags=hidden | immutable)

    copy_metadata(src, dst)

    assert applied == [(str(dst), hidden)]
    # Compared with the source rather than 0o640: Windows ``chmod`` only toggles read-only.
    assert stat.S_IMODE(os.stat(dst).st_mode) == stat.S_IMODE(os.stat(src).st_mode)
    assert os.stat(dst).st_mtime == old_mtime


def test_copy_metadata_still_raises_unrelated_permission_errors(tmp_path, mocker):
    src = tmp_path / "src.bin"
    src.write_text("src")
    dst = tmp_path / "dst.bin"
    dst.write_text("dst")
    denied = PermissionError(1, "Operation not permitted")
    mocker.patch.object(os, "chflags", create=True, side_effect=denied)
    mocker.patch("ocopy.verified_copy.copystat", side_effect=denied)
    mocker.patch.object(os, "utime", side_effect=denied)

    with pytest.raises(PermissionError):
        copy_metadata(src, dst)


def test_copy_metadata_never_sets_locking_flags(tmp_path, mocker):
    """A locking flag is never applied to ``dst``, not even briefly.

    Setting it and clearing it afterwards does not work on an SMB share: macOS
    maps ``UF_IMMUTABLE`` to the DOS read-only attribute, the next open and close
    of the file (the verification read) brings it back, and the rename of the
    ``.copy_in_progress`` temp fails with ``EPERM``. Seen with a GoPro card's
    ``.url`` shortcuts copied to a NAS.
    """
    src = tmp_path / "Get_started_with_GoPro.url"
    src.write_text("[InternetShortcut]\n")
    dst = tmp_path / "dst.url.copy_in_progress"
    dst.write_text("[InternetShortcut]\n")
    immutable = getattr(stat, "UF_IMMUTABLE", 0x2)
    applied = _simulate_exfat_archive_flags(mocker, {src}, extra_flags=immutable, archive_flag=0)

    copy_metadata(src, dst)

    assert not [flags for _, flags in applied if flags & immutable]


def test_exfat_archived_files_are_copied(tmp_path, mocker):
    """A card whose files carry ``SF_ARCHIVED`` copies completely, as on a GoPro card."""
    src = tmp_path / "src"
    (src / "DCIM").mkdir(parents=True)
    archived = {src / "Get_started_with_GoPro.url", src / "DCIM" / "leinfo.sav"}
    for path in archived:
        path.write_text("archived")
    (src / "mdb_v01.db").write_text("plain")
    _simulate_exfat_archive_flags(mocker, archived)

    dst = tmp_path / "dst"
    dst.mkdir()
    copytree(src, [dst])

    assert (dst / "Get_started_with_GoPro.url").read_text() == "archived"
    assert (dst / "DCIM" / "leinfo.sav").read_text() == "archived"
    assert (dst / "mdb_v01.db").read_text() == "plain"
    assert not list(dst.rglob("*.copy_in_progress"))


def test_failed_cleanup_does_not_hide_the_original_error(tmp_path, mocker):
    """The failure that aborted a copy is reported, not a later failure to remove its temps.

    A temp locked on an SMB share could be neither renamed nor removed. The
    ``unlink`` error replaced the rename error, so the log only named the temp
    and hid which step had failed. The other temps are still removed.
    """
    src = tmp_path / "clip.mp4"
    src.write_text("clip")
    dst_1 = tmp_path / "dst_1"
    dst_2 = tmp_path / "dst_2"
    dst_1.mkdir()
    dst_2.mkdir()
    locked_tmp = dst_1 / "clip.mp4.copy_in_progress"
    rename_error = PermissionError(1, "Operation not permitted", str(locked_tmp), str(dst_1 / "clip.mp4"))
    mocker.patch("ocopy.verified_copy._rename_tmps", side_effect=rename_error)
    real_unlink = Path.unlink

    def unlink(self, *args, **kwargs):
        if self == locked_tmp:
            raise PermissionError(1, "Operation not permitted", str(self))
        return real_unlink(self, *args, **kwargs)

    mocker.patch.object(Path, "unlink", unlink)

    with pytest.raises(PermissionError) as excinfo:
        verified_copy(src, [dst_1 / "clip.mp4", dst_2 / "clip.mp4"])

    assert excinfo.value is rename_error
    assert not (dst_2 / "clip.mp4.copy_in_progress").exists()


def test_folder_size_skips_appledouble_files(tmp_path):
    """Progress totals count only what ``copytree`` copies, so AppleDouble files are left out."""
    (tmp_path / "DCIM").mkdir()
    (tmp_path / "DCIM" / "clip.mp4").write_bytes(b"x" * 100)
    (tmp_path / "DCIM" / "._clip.mp4").write_bytes(b"x" * 4096)

    assert folder_size(tmp_path) == 100
