from pathlib import Path

from click.testing import CliRunner

from ocopy.cli.ocopy import cli


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, "--help")
    assert result.exit_code == 0


def test_copy(card):
    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 0
    assert "different drives" in result.output


def test_not_enough_space(card, mocker):
    class MockUsage:
        def __init__(self, path):
            if 'dst_3' in Path(path).as_posix():
                self.free = 0
            else:
                self.free = 1024 * 1024 * 1024 * 1024

    mocker.patch("shutil.disk_usage", MockUsage, create=True)

    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 1
    assert "free space" in result.output
