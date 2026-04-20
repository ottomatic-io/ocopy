import random

import pytest


@pytest.fixture(autouse=True)
def _clear_hash_caches():
    from ocopy.hash import _cached_load_ascmhl
    from ocopy.mhl import _cached_load_mhl_index

    for fn in (_cached_load_ascmhl, _cached_load_mhl_index):
        fn.cache_clear()
    yield


@pytest.fixture
def card(tmp_path):
    src_dir = tmp_path / "src"
    for card_number in range(1, 3):
        card = src_dir / f"A00{card_number}XXXX"
        card.mkdir(parents=True)

        for clip_number in range(1, 5):
            data = random.randint(0, 100) * b"X"
            (card / f"A00{card_number}C00{clip_number}_XXXX_XXXX.mov").write_bytes(data)

    destinations = [tmp_path / f"dst_{i}" for i in range(1, 4)]
    for d in destinations:
        d.mkdir()

    return src_dir, destinations
