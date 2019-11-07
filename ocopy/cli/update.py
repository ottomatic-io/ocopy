from distutils.version import LooseVersion
from threading import Thread

import pkg_resources
import requests


class Updater(Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True
        self.latest_version = None
        self.installed_version = None
        self.finished = False
        self.start()

    def run(self):
        self._get_latest_version()
        self._get_installed_version()
        self.finished = True

    @property
    def needs_update(self) -> bool:
        if not self.latest_version or not self.installed_version:
            return False

        return self.latest_version > self.installed_version

    def _get_latest_version(self):
        try:
            r = requests.get("https://api.github.com/repos/OTTOMATIC-IO/ocopy/releases/latest")
            r.raise_for_status()
            self.latest_version = LooseVersion(r.json().get("tag_name"))
        except requests.exceptions.RequestException:
            self.finished = True

    def _get_installed_version(self):
        try:
            self.installed_version = LooseVersion(pkg_resources.get_distribution("ocopy").version)
        except pkg_resources.DistributionNotFound:
            self.finished = True
