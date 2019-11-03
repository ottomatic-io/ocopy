import random
from pathlib import Path

import pytest


@pytest.fixture
def card(tmpdir):
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

    return src_dir, destinations
