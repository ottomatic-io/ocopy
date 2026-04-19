import logging
import sys
from importlib.metadata import PackageNotFoundError, distributions
from importlib.metadata import version as get_version
from pathlib import Path
from threading import Thread

import requests
from packaging.version import InvalidVersion, Version

logger = logging.getLogger(__name__)


def _normalize_dist_name(name: str) -> str:
    return (name or "").lower().replace("-", "_")


def _read_installer(dist_name: str) -> str | None:
    """Read the PEP 376 ``INSTALLER`` file when present.

    ``importlib.metadata.distribution()`` can resolve editable installs to a legacy
    ``.egg-info`` tree that has no ``INSTALLER``, while a parallel ``.dist-info`` in
    ``site-packages`` does — so we scan distributions and prefer ``.dist-info``.
    """
    want = _normalize_dist_name(dist_name)
    fallback: str | None = None
    for dist in distributions():
        try:
            name = dist.metadata["Name"]
        except KeyError:
            name = ""
        if _normalize_dist_name(name) != want:
            continue
        text = dist.read_text("INSTALLER")
        if not text:
            continue
        value = _normalize_installer_label(text)
        if not value:
            continue
        if ".dist-info" in str(getattr(dist, "_path", "")):
            return value
        fallback = fallback or value
    return fallback


def _normalize_installer_label(text: str) -> str | None:
    """First line, first word, lowercased (e.g. ``Poetry 2`` → ``poetry``)."""
    line = text.strip().splitlines()[0].strip().lower() if text.strip() else ""
    if not line:
        return None
    return line.split()[0]


def _is_conda_env() -> bool:
    """True when running inside a conda / mamba / micromamba environment."""
    return (Path(sys.prefix).resolve() / "conda-meta").is_dir()


def _is_uv_tool_environment() -> bool:
    """True when ocopy was installed with ``uv tool install`` (not ``uv pip`` / pip)."""
    receipt = Path(sys.prefix).resolve() / "uv-receipt.toml"
    if receipt.is_file():
        try:
            return receipt.read_text(encoding="utf-8").lstrip().startswith("[tool]")
        except OSError:
            return False
    try:
        import ocopy

        p = Path(ocopy.__file__).resolve().as_posix()
        if "/uv/tools/" in p.replace("\\", "/"):
            return True
    except Exception:
        pass
    return False


def suggested_update_command() -> str:
    # INSTALLER values (ocopy 0.8.0, manual check): pip→pip; uv pip & uv tool→uv
    # (tool env uses receipt above); Poetry→"Poetry x.y"→poetry; PDM→pdm; Pipenv→pip.
    if _is_uv_tool_environment():
        return "uv tool upgrade ocopy"
    installer = _read_installer("ocopy")
    if installer == "uv":
        return "uv pip install -U ocopy"
    if installer == "pip":
        return "python -m pip install -U ocopy" if _is_conda_env() else "pip3 install -U ocopy"
    if installer == "poetry":
        return "poetry update ocopy"
    if installer == "pdm":
        return "pdm update ocopy"
    if installer == "conda":
        # ocopy is not published as a conda package; pip inside the env is the practical path.
        return "python -m pip install -U ocopy"
    if installer is None and _is_conda_env():
        return "python -m pip install -U ocopy"
    # Unknown installer outside conda: README default (uv tool install).
    return "uv tool upgrade ocopy"


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
