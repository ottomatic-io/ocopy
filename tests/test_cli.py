import random
from pathlib import Path

from click.testing import CliRunner

from ocopy.cli.ocopy import cli


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, "--help")
    assert result.exit_code == 0


def test_copy(tmpdir):
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

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 0
    assert "different drives" in result.output
