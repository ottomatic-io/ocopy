import time

import requests
from packaging.version import Version


def test_updater(requests_mock, mocker):
    requests_mock.get("https://api.github.com/repos/OTTOMATIC-IO/ocopy/releases/latest", json={"tag_name": "0.6.5"})

    from importlib import reload

    import ocopy.cli.update

    reload(ocopy.cli.update)
    mocker.patch.object(ocopy.cli.update, "get_version", return_value="0.0.1")

    updater = ocopy.cli.update.Updater()
    while not updater.finished:
        time.sleep(0.1)

    assert updater.latest_version == Version("0.6.5")
    assert updater.installed_version == Version("0.0.1")
    assert updater.needs_update is True
    updater.join(timeout=1)


def test_updater_timeout(requests_mock, mocker):
    requests_mock.get(
        "https://api.github.com/repos/OTTOMATIC-IO/ocopy/releases/latest", exc=requests.exceptions.ConnectTimeout
    )

    from importlib import reload

    import ocopy.cli.update

    reload(ocopy.cli.update)
    mocker.patch.object(ocopy.cli.update, "get_version", return_value="0.0.1")

    updater = ocopy.cli.update.Updater()
    while not updater.finished:
        time.sleep(0.1)

    assert updater.latest_version is None
    assert updater.needs_update is False
    updater.join(timeout=1)
