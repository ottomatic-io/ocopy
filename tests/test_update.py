import time
from distutils.version import LooseVersion

import requests


def test_updater(requests_mock, mocker):
    class Distribution:
        def __init__(self, _):
            self.version = "0.0.1"

    mocker.patch("pkg_resources.get_distribution", Distribution)
    requests_mock.get("https://api.github.com/repos/OTTOMATIC-IO/ocopy/releases/latest", json={"tag_name": "0.6.5"})

    from importlib import reload
    import ocopy.cli.update

    reload(ocopy.cli.update)

    updater = ocopy.cli.update.Updater()
    while not updater.finished:
        time.sleep(0.1)

    assert updater.latest_version == LooseVersion("0.6.5")
    assert updater.installed_version == LooseVersion("0.0.1")
    assert updater.needs_update is True
    updater.join(timeout=1)


def test_updater_timeout(requests_mock, mocker):
    class Distribution:
        def __init__(self, _):
            self.version = "0.0.1"

    mocker.patch("pkg_resources.get_distribution", Distribution)

    requests_mock.get(
        "https://api.github.com/repos/OTTOMATIC-IO/ocopy/releases/latest", exc=requests.exceptions.ConnectTimeout
    )

    from importlib import reload
    import ocopy.cli.update

    reload(ocopy.cli.update)

    updater = ocopy.cli.update.Updater()
    while not updater.finished:
        time.sleep(0.1)

    assert updater.latest_version is None
    assert updater.needs_update is False
    updater.join(timeout=1)
