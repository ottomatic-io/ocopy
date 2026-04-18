"""Checkpoint resume: interrupted runs re-use verified digests without re-reading bytes.

Each test seeds the destination tree + ``.ocopy-checkpoint`` by hand to simulate an
interrupted prior run. Doing so keeps the checkpoint - not an ASC MHL manifest - as
the sole trust source, so the assertions actually exercise the resume path.
"""

import importlib
from shutil import copystat

from ocopy.checkpoint import Checkpoint
from ocopy.hash import get_hash
from ocopy.verified_copy import copy_and_seal


def _seed_resume_state(tmp_path, names: list[str]):
    """Create ``src/`` with the given files plus a matching destination + checkpoint.

    Returns ``(src, dest_parent, root, files, digests)``. No manifest is written;
    the only hash-bearing sidecar is the checkpoint.
    """
    src = tmp_path / "src"
    src.mkdir()
    files = []
    for i, name in enumerate(names):
        f = src / name
        f.write_bytes(name.encode() * (i + 4))
        files.append(f)

    dest_parent = tmp_path / "d1"
    dest_parent.mkdir()
    root = dest_parent / "src"
    root.mkdir()
    for f in files:
        dst = root / f.name
        dst.write_bytes(f.read_bytes())
        copystat(f, dst)

    # Hash with the real implementation before installing any spy so the digests
    # we record are real and the later copy_and_seal has nothing left to hash.
    digests = [get_hash(f) for f in files]

    cp = Checkpoint(root)
    cp.ensure_exists()
    for f, h in zip(files, digests, strict=True):
        st = f.stat()
        cp.record(f.name, st.st_size, st.st_mtime, h)

    return src, dest_parent, root, files, digests


def test_resume_uses_checkpoint_without_manifest(tmp_path, mocker):
    """Checkpoint alone (no MHL) must drive a zero-read fast-skip on resume."""
    src, dest_parent, root, _, _ = _seed_resume_state(tmp_path, ["a.bin", "b.bin"])
    assert not (root / "ascmhl").exists(), "precondition: no manifest yet"
    assert (root / Checkpoint.FILENAME).is_file(), "precondition: checkpoint seeded"

    spy = mocker.spy(importlib.import_module("ocopy.hash"), "get_hash")
    copy_and_seal(src, [dest_parent], skip_existing=True)

    assert spy.call_count == 0, "resume must not re-hash any byte when checkpoint is trusted"
    assert (root / "ascmhl").is_dir(), "successful run should seal a manifest"
    assert not (root / Checkpoint.FILENAME).exists(), "checkpoint must be cleared after seal"


def test_resume_rehashes_only_files_with_truncated_checkpoint(tmp_path, mocker):
    """Tampering with the checkpoint's last record must re-hash only that file."""
    src, dest_parent, root, files, _ = _seed_resume_state(tmp_path, ["a.bin", "b.bin", "c.bin"])
    cp = root / Checkpoint.FILENAME

    # Preserve the first two records whole; chop the third mid-JSON.
    raw = cp.read_bytes()
    nls = [i for i, byte in enumerate(raw) if byte == ord(b"\n")]
    assert len(nls) >= 3, "precondition: checkpoint has three records"
    cp.write_bytes(raw[: nls[1] + 1] + b'{"rel_path":"c.bin","siz')

    spy = mocker.spy(importlib.import_module("ocopy.hash"), "get_hash")
    copy_and_seal(src, [dest_parent], skip_existing=True)

    # Only ``c.bin`` loses its checkpoint entry -> pool verify reads source + dest.
    # The other two files ride the checkpoint and are never re-hashed.
    c_path = files[-1]
    hashed = {call.args[0] for call in spy.call_args_list}
    assert c_path in hashed, "orphaned file must be re-hashed"
    assert len(hashed & {files[0], files[1]}) == 0, "checkpointed files must not be re-hashed"
    assert (root / "ascmhl").is_dir()
    assert not (root / Checkpoint.FILENAME).exists()


def test_checkpoint_cleared_after_successful_seal(tmp_path):
    """Successful seal must clean the sidecar; next run should find no stale state."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "f.bin").write_bytes(b"q")

    dest_parent = tmp_path / "d1"
    dest_parent.mkdir()
    copy_and_seal(src, [dest_parent], skip_existing=True)
    assert not (dest_parent / "src" / Checkpoint.FILENAME).exists()
