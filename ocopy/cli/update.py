import logging
from importlib.metadata import PackageNotFoundError, version as get_version
from threading import Thread

import requests
from packaging.version import InvalidVersion, Version

logger = logging.getLogger(__name__)


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
            tag_name = r.json().get("tag_name")
        except requests.exceptions.RequestException:
            self.finished = True
            return

        self.latest_version = self._parse_version(tag_name)

    @staticmethod
    def _parse_version(tag_name):
        if not tag_name:
            logger.warning("No tag_name returned from GitHub releases API")
            return None

        try:
            return Version(tag_name)
        except InvalidVersion:
            pass

        normalized = tag_name.strip().lstrip("vV")
        try:
            return Version(normalized)
        except InvalidVersion:
            logger.warning("Could not parse GitHub release tag as a version: %r", tag_name)
            return None

    def _get_installed_version(self):
        try:
            self.installed_version = Version(get_version("ocopy"))
        except PackageNotFoundError:
            self.finished = True
