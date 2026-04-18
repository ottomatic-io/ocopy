import pytest
from click.testing import CliRunner

from ocopy.cli.ocopy import cli
from ocopy.sleep_inhibit import sleep_inhibit_best_effort


def test_sleep_inhibit_smoke():
    with sleep_inhibit_best_effort():
        pass


def test_sleep_inhibit_enters_wakepy(mocker):
    fake_cm = mocker.MagicMock()
    fake_cm.__enter__.return_value = None
    fake_cm.__exit__.return_value = None
    mocker.patch("wakepy.keep.running", return_value=fake_cm)

    with sleep_inhibit_best_effort():
        pass

    fake_cm.__enter__.assert_called_once()
    fake_cm.__exit__.assert_called_once()


def test_sleep_inhibit_warns_when_enter_fails(mocker):
    fake_cm = mocker.MagicMock()
    fake_cm.__enter__.side_effect = RuntimeError("no session bus")
    fake_cm.__exit__.return_value = None
    mocker.patch("wakepy.keep.running", return_value=fake_cm)
    warn = mocker.Mock()

    ran = False
    with sleep_inhibit_best_effort(warn=warn):
        ran = True

    assert ran is True
    warn.assert_called_once()
    assert "Could not inhibit system sleep" in warn.call_args[0][0]


def test_sleep_inhibit_body_exception_still_exits_wakepy(mocker):
    class TrackingCM:
        def __init__(self) -> None:
            self.exit_args: tuple | None = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.exit_args = (exc_type, exc, tb)
            return False

    cm = TrackingCM()
    mocker.patch("wakepy.keep.running", return_value=cm)

    with pytest.raises(ValueError), sleep_inhibit_best_effort():
        raise ValueError("boom")

    assert cm.exit_args is not None
    assert cm.exit_args[0] is ValueError


def test_cli_enters_wakepy_during_copy(card, mocker):
    fake_cm = mocker.MagicMock()
    fake_cm.__enter__.return_value = None
    fake_cm.__exit__.return_value = None
    mocker.patch("wakepy.keep.running", return_value=fake_cm)

    src_dir, destinations = card

    runner = CliRunner()
    result = runner.invoke(cli, [src_dir.as_posix(), *[d.as_posix() for d in destinations]])
    assert result.exit_code == 0
    fake_cm.__enter__.assert_called_once()
    fake_cm.__exit__.assert_called_once()
