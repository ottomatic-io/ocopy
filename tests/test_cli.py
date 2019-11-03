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
