"""Tests for mid-copy destination failure reporting.

Covers the scenario where a destination drive unmounts or otherwise becomes
unavailable while a ``CopyJob`` is running. The failure must surface on
``CopyJob.errors`` and must not silently land on the underlying local FS.

Originally motivated by https://github.com/ottomatic-io/ocopy/issues/15.

Two tests live here:

1. A fast mocked test that raises ``OSError(ENODEV)`` on destination writes. This
   covers the portable "drive errors out" variant and is the actual regression guard.
2. A macOS-only empirical probe that creates a real disk image, lets ``hdiutil``
   mount it under ``/Volumes/<volname>`` the same way the system does for real
   drives, runs a ``CopyJob`` against that mount, force-detaches mid-copy, and
   records whether the failure is observable to ocopy or whether any files slipped
   onto the local FS.
"""

from __future__ import annotations

import builtins
import errno
import os
import shutil
import subprocess
import sys
import threading
import uuid
from pathlib import Path

import pytest

from ocopy.verified_copy import CopyJob


def test_destination_unmount_mid_copy_surfaces_error(tmp_path, mocker):
    """Issue #15: mid-copy destination I/O failure must surface on ``CopyJob.errors``."""
    src = tmp_path / "src"
    src.mkdir()
    for i in range(4):
        (src / f"clip_{i}.mov").write_bytes(b"x" * 8_000)

    destinations = [tmp_path / "dst_ok", tmp_path / "dst_victim"]
    for d in destinations:
        d.mkdir()
    victim = destinations[1]

    real_open = builtins.open
    victim_writes_seen = 0

    def fake_open(file, *args, **kwargs):
        nonlocal victim_writes_seen
        path_str = str(file)
        if str(victim) in path_str and ".copy_in_progress" in path_str:
            victim_writes_seen += 1
            if victim_writes_seen > 1:
                raise OSError(errno.ENODEV, "No such device", path_str)
        return real_open(file, *args, **kwargs)

    mocker.patch("builtins.open", side_effect=fake_open)

    job = CopyJob(src, destinations, verify=True, mhl=False)
    job.join(timeout=60)

    assert job.finished, "CopyJob did not finish within timeout (possible hang under destination failure)"
    assert job.errors, "expected CopyJob.errors to surface the destination I/O failure (issue #15)"
    assert any("No such device" in e.error_message for e in job.errors), (
        f"expected the ENODEV message to appear in job.errors, got: {[e.error_message for e in job.errors]}"
    )


def _hdiutil(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["hdiutil", *args], check=True, capture_output=True, text=True)


def _detach_best_effort(mountpoint: Path) -> None:
    subprocess.run(
        ["hdiutil", "detach", str(mountpoint), "-force"],
        capture_output=True,
        check=False,
    )


@pytest.mark.skipif(sys.platform != "darwin", reason="hdiutil-based probe is macOS-only")
def test_real_detach_mid_copy_is_reported_no_silent_writes(tmp_path, mocker):
    """Empirical probe of issue #15 using the realistic macOS mount lifecycle.

    No ``-mountpoint`` flag: ``hdiutil attach`` mounts under ``/Volumes/<volname>``, the
    way the system mounts any removable drive. After ``hdiutil detach -force`` the
    system removes that directory. With ``/Volumes`` owned ``root:wheel 0755`` on
    modern macOS, a non-root process cannot recreate it, so the "silent writes to a
    local-FS directory" failure mode described in the original report is only possible
    if ``/Volumes`` is world-writable (older macOS, custom setups).

    The source tree deliberately has nested subdirectories so ``copytree`` recurses
    and calls ``mkdir(parents=True, exist_ok=True)`` on each subdirectory *after* the
    detach -- that is the exact call that would trigger a silent local-FS write if the
    parent were writable.
    """
    volname = f"ocopytest_{uuid.uuid4().hex[:8]}"
    mountpoint = Path(f"/Volumes/{volname}")
    dmg = tmp_path / "volume.sparseimage"

    local_dev = tmp_path.stat().st_dev
    volumes_dir_mode = oct(Path("/Volumes").stat().st_mode & 0o7777)

    job: CopyJob | None = None
    image_dev: int | None = None
    mounted_write_started = threading.Event()
    detach_complete = threading.Event()

    try:
        _hdiutil("create", "-size", "512m", "-fs", "HFS+", "-volname", volname, str(dmg))
        _hdiutil("attach", str(dmg), "-nobrowse", "-noautoopen")
    except (FileNotFoundError, subprocess.CalledProcessError) as err:
        # Some CI environments (sandboxed macOS runners, restricted containers) deny
        # hdiutil. That's an environment limitation, not a regression of the code
        # under test, so we skip rather than fail.
        pytest.skip(f"hdiutil not usable in this environment: {err}")

    try:
        assert mountpoint.exists(), "hdiutil did not create the expected /Volumes mountpoint"
        image_dev = mountpoint.stat().st_dev
        assert image_dev != local_dev, "precondition failed: attach did not create a new device node"

        src = tmp_path / "src"
        src.mkdir()
        # Nested subdirectories so copytree has to recurse and mkdir each one. The
        # later cards are the ones that will be touched post-detach and are the
        # candidates for a silent-write regression.
        for card in ("A", "B", "C", "D", "E", "F", "G", "H"):
            card_dir = src / f"card_{card}"
            card_dir.mkdir()
            for i in range(12):
                # Large enough that verify+dual-destination work does not finish before we
                # observe a .copy_in_progress on the volume (fast CI runners).
                (card_dir / f"clip_{i:03d}.mov").write_bytes(os.urandom(1_500_000))

        dst_ok = tmp_path / "dst_ok"
        dst_ok.mkdir()

        real_open = builtins.open

        class _GateMountedWrite:
            def __init__(self, wrapped):
                self._wrapped = wrapped
                self._gated = False

            def write(self, data):
                if not self._gated:
                    self._gated = True
                    mounted_write_started.set()
                    assert detach_complete.wait(timeout=30), "timed out waiting to detach mounted destination"
                return self._wrapped.write(data)

            def __enter__(self):
                self._wrapped.__enter__()
                return self

            def __exit__(self, exc_type, exc, tb):
                return self._wrapped.__exit__(exc_type, exc, tb)

            def __getattr__(self, name):
                return getattr(self._wrapped, name)

        def fake_open(file, *args, **kwargs):
            handle = real_open(file, *args, **kwargs)
            path_str = os.fspath(file)
            mode = args[0] if args else kwargs.get("mode", "r")
            if path_str.startswith(os.fspath(mountpoint)) and ".copy_in_progress" in path_str and "w" in mode:
                return _GateMountedWrite(handle)
            return handle

        mocker.patch("builtins.open", side_effect=fake_open)

        job = CopyJob(src, [dst_ok, mountpoint], verify=True, mhl=False, auto_start=False)
        job.start()

        # Detach on the first actual mounted-volume write, not a polled filesystem
        # observation that can notice too late on fast runners.
        assert mounted_write_started.wait(timeout=120), "timed out waiting for the first mounted destination write"
        _detach_best_effort(mountpoint)
        detach_complete.set()

        job.join(timeout=180)
    finally:
        _detach_best_effort(mountpoint)
        # If anything managed to create /Volumes/<volname> as a regular directory
        # (the silent-write failure mode), clean it up to avoid polluting the system.
        if mountpoint.exists():
            shutil.rmtree(mountpoint, ignore_errors=True)

    assert job is not None and image_dev is not None, "test setup did not reach CopyJob"
    assert job.finished, "CopyJob did not finish within timeout after detach"

    mountpoint_after = mountpoint.exists()
    files_on_local_at_mountpath: list[Path] = []
    if mountpoint_after:
        try:
            mp_dev = mountpoint.stat().st_dev
            if mp_dev == local_dev:
                files_on_local_at_mountpath = [p for p in mountpoint.rglob("*") if p.is_file()]
        except OSError:
            pass

    silent_finals_on_local_fs = [p for p in files_on_local_at_mountpath if ".copy_in_progress" not in p.name]

    report_lines = [
        "",
        f"/Volumes mode:                               {volumes_dir_mode}",
        f"mountpoint path:                             {mountpoint}",
        f"mountpoint exists after detach:              {mountpoint_after}",
        f"local (tmp_path) device:                     {local_dev}",
        f"disk image device (while mounted):           {image_dev}",
        f"# files under mountpoint on local FS:        {len(files_on_local_at_mountpath)}",
        f"# of those finalized (silent-write suspect): {len(silent_finals_on_local_fs)}",
        f"# job.errors surfaced:                       {len(job.errors)}",
        *(f"  - {e.source.name}: {e.error_message}" for e in job.errors[:5]),
    ]
    report = "\n".join(report_lines)
    print(report)

    if silent_finals_on_local_fs:
        pytest.fail(
            f"Issue #15 reproduced empirically: {len(silent_finals_on_local_fs)} file(s) landed on "
            f"the LOCAL FS at the mountpoint path after detach, while CopyJob reported "
            f"{len(job.errors)} error(s)." + report
        )

    # Positive regression guard: if no silent writes occurred, the detach must still
    # have been observable to the caller via ``job.errors``. A future change that
    # causes a mid-copy detach to produce neither silent writes NOR errors would be
    # the exact "no error, had to abort" regression that issue #15 is about.
    assert job.errors, (
        "Force-detach mid-copy produced no silent writes AND no CopyJob.errors; "
        "the failure went completely undetected." + report
    )
