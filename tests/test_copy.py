import os
import random
from pathlib import Path
from shutil import copystat

import pytest

from ocopy.copy import copy, copytree, copy_and_seal
from ocopy.hash import get_hash
from ocopy.progress import PROGRESS_QUEUE
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


def test_copytree(tmpdir):
    tmpdir = Path(tmpdir)
    src_dir = tmpdir / "src"
    for card_number in range(1, 3):
        card = src_dir / f"A00{card_number}XXXX"
        card.mkdir(parents=True)

        for clip_number in range(1, 5):
            data = random.randint(0, 100) * b"X"
            (card / f"A00{card_number}C00{clip_number}_XXXX_XXXX.mov").write_bytes(data)

    destinations = [tmpdir / f"dst_{i}" for i in range(1, 4)]
    for d in destinations:
        d.mkdir()

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

    destination = Path(tmpdir) / "dest_x"
    src_folder = src_dir / "XYZ"
    dst_folder = destination / "XYZ"
    src_folder.mkdir()
    dst_folder.mkdir(parents=True)

    (src_folder / "existing_file").write_text("foo")
    (dst_folder / "existing_file").write_text("foo")

    # FIXME: Expect FileExistsError
    with pytest.raises(Exception):
        copytree(src_dir, [destination])

    # Only skip when the modification times match
    os.utime((dst_folder / "existing_file"), (0, 0))
    with pytest.raises(Exception):
        copytree(src_dir, [destination], skip_existing=True)

    # Make the mtime match
    copystat(src_folder / "existing_file", dst_folder / "existing_file")
    copytree(src_dir, [destination], skip_existing=True)

    # Just overwrite existing files
    copytree(src_dir, [destination], overwrite=True)


def test_copy_and_seal(tmpdir):
    tmpdir = Path(tmpdir)
    src_dir = tmpdir / "src"
    for card_number in range(1, 3):
        card = src_dir / f"A00{card_number}XXXX"
        card.mkdir(parents=True)

        for clip_number in range(1, 5):
            data = random.randint(0, 100) * b"X"
            (card / f"A00{card_number}C00{clip_number}_XXXX_XXXX.mov").write_bytes(data)

    (src_dir / ".DS_Store").write_text("")
    (src_dir / ".some_hidden_file").write_text("")

    destinations = [tmpdir / f"dst_{i}" for i in range(1, 4)]
    for d in destinations:
        d.mkdir()

    copy_and_seal(src_dir, destinations)

    while True:
        file_path, done = PROGRESS_QUEUE.get(timeout=5)
        if file_path == "finished":
            break

    for dest in destinations:
        assert len(list((dest / "src").glob("*.mhl"))) == 1
        assert len((dest / "src" / "xxHash.txt").read_text().splitlines()) == 9
        assert ".DS_Store" not in [e.name for e in dest.glob("**/*")]
        assert ".some_hidden_file" in [e.name for e in dest.glob("**/*")]

    PROGRESS_QUEUE.close()
