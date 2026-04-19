import random
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from ocopy.backup_check import get_missing


@pytest.fixture()
def data():
    with TemporaryDirectory() as tmp_dirname:
        # Create a fake card and backup drive
        card_name = "A001XXXX"
        tmp = Path(tmp_dirname)
        card = tmp / card_name
        backup = tmp / "BACKUP"
        card.mkdir()
        backup.mkdir()

        # Add some files with random data to the card
        for i in range(1, 5):
            data = random.randint(0, 100) * b"X"
            (card / f"A001C00{i}_XXXX_XXXX.mov").write_bytes(data)

        # Copy the files to the destination
        backup_destination = backup / "some" / "tree" / "structure" / card_name
        shutil.copytree(card, backup_destination)

        yield card, backup, backup_destination


def test_basic(data):
    card, backup, _backup_destination = data

    # No files should be missing
    assert get_missing(str(card), str(backup)) == ([], 4)


def test_missing(data):
    card, backup, backup_destination = data

    # Remove one file and make sure it's missing
    (backup_destination / "A001C002_XXXX_XXXX.mov").unlink()
    assert get_missing(str(card), str(backup)) == (["A001C002_XXXX_XXXX.mov"], 4)


def test_altered(data):
    card, backup, backup_destination = data

    # Change one file and make sure it's missing
    clip = backup_destination / "A001C002_XXXX_XXXX.mov"
    clip.write_bytes(clip.read_bytes() + b"X")
    assert get_missing(str(card), str(backup)) == (["A001C002_XXXX_XXXX.mov"], 4)


def test_ignored_source_file_not_required(tmp_path):
    """Basenames in ``ignored_paths`` are not counted as required on the destination."""
    card_name = "A001XXXX"
    card = tmp_path / card_name
    backup = tmp_path / "BACKUP"
    card.mkdir()
    backup.mkdir()
    for i in range(1, 5):
        (card / f"A001C00{i}_XXXX_XXXX.mov").write_bytes(b"x" * 100)
    (card / "SONYCARD.IND").write_bytes(b"ignored")
    backup_destination = backup / "some" / "tree" / "structure" / card_name
    shutil.copytree(card, backup_destination)
    assert get_missing(str(card), str(backup)) == ([], 4)


def test_destination_ascmhl_folder_ignored(tmp_path):
    """Manifest output under ``ascmhl/`` must not affect the missing-file check."""
    card_name = "A001XXXX"
    card = tmp_path / card_name
    backup = tmp_path / "BACKUP"
    card.mkdir()
    backup.mkdir()
    for i in range(1, 5):
        (card / f"A001C00{i}_XXXX_XXXX.mov").write_bytes(b"x" * 100)
    backup_destination = backup / "some" / "tree" / "structure" / card_name
    shutil.copytree(card, backup_destination)
    ascmhl = backup_destination / "ascmhl"
    ascmhl.mkdir()
    (ascmhl / "gen.mhl").write_bytes(b"fake manifest")
    assert get_missing(str(card), str(backup)) == ([], 4)
