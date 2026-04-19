import time

import requests
from packaging.version import Version

import ocopy
from ocopy.cli.update import (
    Updater,
    _is_uv_tool_environment,
    _normalize_installer_label,
    _read_installer,
    suggested_update_command,
)


def test_parse_version_accepts_plain_pep440():
    assert Updater._parse_version("0.6.5") == Version("0.6.5")


def test_parse_version_strips_leading_v_prefix():
    assert Updater._parse_version("v0.6.5") == Version("0.6.5")
    assert Updater._parse_version("V1.0.0") == Version("1.0.0")


def test_parse_version_none_or_invalid_returns_none():
    assert Updater._parse_version(None) is None
    assert Updater._parse_version("") is None
    assert Updater._parse_version("not-a-version") is None


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


def test_read_installer_returns_normalized_name(mocker):
    mock_dist = mocker.Mock()
    mock_dist.metadata = {"Name": "ocopy"}
    mock_dist.read_text = mocker.Mock(return_value="  UV \n")
    mock_dist._path = "/fake/ocopy-1.0.dist-info"
    mocker.patch("ocopy.cli.update.distributions", return_value=[mock_dist])
    assert _read_installer("ocopy") == "uv"


def test_read_installer_missing_returns_none(mocker):
    mock_dist = mocker.Mock()
    mock_dist.metadata = {"Name": "ocopy"}
    mock_dist.read_text = mocker.Mock(return_value=None)
    mock_dist._path = "/fake/ocopy-1.0.dist-info"
    mocker.patch("ocopy.cli.update.distributions", return_value=[mock_dist])
    assert _read_installer("ocopy") is None


def test_normalize_installer_label_first_word():
    assert _normalize_installer_label("Poetry 2.1.0\n") == "poetry"
    assert _normalize_installer_label("  pip \n") == "pip"


def test_read_installer_prefers_dist_info_over_egg_info(mocker):
    egg = mocker.Mock()
    egg.metadata = {"Name": "ocopy"}
    egg.read_text = mocker.Mock(return_value=None)
    egg._path = "/src/ocopy.egg-info"
    wheel = mocker.Mock()
    wheel.metadata = {"Name": "ocopy"}
    wheel.read_text = mocker.Mock(return_value="pip\n")
    wheel._path = "/env/lib/site-packages/ocopy-1.0.dist-info"
    mocker.patch("ocopy.cli.update.distributions", return_value=[egg, wheel])
    assert _read_installer("ocopy") == "pip"


def test_is_uv_tool_true_when_receipt_has_tool_section(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    (tmp_path / "uv-receipt.toml").write_text("[tool]\nrequirements = []\n", encoding="utf-8")
    assert _is_uv_tool_environment() is True


def test_is_uv_tool_false_without_receipt(tmp_path, monkeypatch, mocker):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    mocker.patch.object(ocopy, "__file__", str(tmp_path / "site-packages/ocopy/__init__.py"))
    assert _is_uv_tool_environment() is False


def test_is_uv_tool_true_when_under_default_uv_tools_path(tmp_path, monkeypatch, mocker):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    deep = tmp_path / "share/uv/tools/ocopy/lib/python3.12/site-packages/ocopy/__init__.py"
    deep.parent.mkdir(parents=True)
    mocker.patch.object(ocopy, "__file__", str(deep))
    assert _is_uv_tool_environment() is True


def test_suggested_update_command_uv_tool(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    (tmp_path / "uv-receipt.toml").write_text("[tool]\nrequirements = []\n", encoding="utf-8")
    assert suggested_update_command() == "uv tool upgrade ocopy"


def test_suggested_update_command_uv_pip(mocker, tmp_path, monkeypatch):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    mocker.patch("ocopy.cli.update._read_installer", return_value="uv")
    mocker.patch("ocopy.cli.update._is_uv_tool_environment", return_value=False)
    assert suggested_update_command() == "uv pip install -U ocopy"


def test_suggested_update_command_unknown_installer_uses_uv_tool(mocker, tmp_path, monkeypatch):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    mocker.patch("ocopy.cli.update._read_installer", return_value=None)
    mocker.patch("ocopy.cli.update._is_uv_tool_environment", return_value=False)
    mocker.patch("ocopy.cli.update._is_conda_env", return_value=False)
    assert suggested_update_command() == "uv tool upgrade ocopy"


def test_suggested_update_command_explicit_pip(mocker, tmp_path, monkeypatch):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    mocker.patch("ocopy.cli.update._read_installer", return_value="pip")
    mocker.patch("ocopy.cli.update._is_uv_tool_environment", return_value=False)
    mocker.patch("ocopy.cli.update._is_conda_env", return_value=False)
    assert suggested_update_command() == "pip3 install -U ocopy"


def test_suggested_update_command_pip_in_conda_env(mocker, tmp_path, monkeypatch):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    mocker.patch("ocopy.cli.update._read_installer", return_value="pip")
    mocker.patch("ocopy.cli.update._is_uv_tool_environment", return_value=False)
    mocker.patch("ocopy.cli.update._is_conda_env", return_value=True)
    assert suggested_update_command() == "python -m pip install -U ocopy"


def test_suggested_update_command_unknown_installer_in_conda_env(mocker, tmp_path, monkeypatch):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    mocker.patch("ocopy.cli.update._read_installer", return_value=None)
    mocker.patch("ocopy.cli.update._is_uv_tool_environment", return_value=False)
    mocker.patch("ocopy.cli.update._is_conda_env", return_value=True)
    assert suggested_update_command() == "python -m pip install -U ocopy"


def test_suggested_update_command_poetry(mocker, tmp_path, monkeypatch):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    mocker.patch("ocopy.cli.update._read_installer", return_value="poetry")
    mocker.patch("ocopy.cli.update._is_uv_tool_environment", return_value=False)
    assert suggested_update_command() == "poetry update ocopy"


def test_suggested_update_command_pdm(mocker, tmp_path, monkeypatch):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    mocker.patch("ocopy.cli.update._read_installer", return_value="pdm")
    mocker.patch("ocopy.cli.update._is_uv_tool_environment", return_value=False)
    assert suggested_update_command() == "pdm update ocopy"


def test_suggested_update_command_conda_installer(mocker, tmp_path, monkeypatch):
    monkeypatch.setattr("sys.prefix", str(tmp_path))
    mocker.patch("ocopy.cli.update._read_installer", return_value="conda")
    mocker.patch("ocopy.cli.update._is_uv_tool_environment", return_value=False)
    assert suggested_update_command() == "python -m pip install -U ocopy"
