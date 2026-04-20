from pathlib import Path

from ocopy.mhl import find_mhl, get_hash_from_mhl


def test_get_hash_from_mhl():
    mhl_string = """<?xml version='1.0' encoding='utf-8'?>
        <hashlist version="1.0">
          <creatorinfo>
            <name>Ben Hagen</name>
            <username>ben</username>
            <hostname>Bens-MacBook-Pro.local</hostname>
            <tool>o/COPY</tool>
            <startdate>2018-01-07T21:31:17Z</startdate>
            <finishdate>2018-01-07T21:31:52Z</finishdate>
          </creatorinfo>
          <hash>
            <file>big_testfile</file>
            <size>7340032000</size>
            <xxhash64be>4f4a9e4dcc5e6354</xxhash64be>
            <lastmodificationdate>2018-01-05T21:26:59Z</lastmodificationdate>
            <hashdate>2018-01-07T21:31:52Z</hashdate>
          </hash>
        </hashlist>
        """
    assert get_hash_from_mhl(mhl_string, Path("big_testfile")) == "4f4a9e4dcc5e6354"


def test_find_mhl(tmpdir):
    tmpdir = Path(tmpdir)
    mhl_path = tmpdir / "some.mhl"
    mhl_path.write_text("some data")

    file_dir = tmpdir / "some" / "sub" / "dir"
    file_dir.mkdir(parents=True)

    found = find_mhl(file_dir / "some_file.mov")
    assert found is not None
    assert found.read_text() == "some data"


def test_find_mhl_returns_none_when_current_dir_is_own_parent_and_no_mhl(tmp_path, monkeypatch):
    """
    CUB-481: for ``Path('.')``, parent is ``.`` and the cwd is typically not a mount point;
    with no ``*.mhl`` in the tree, ``find_mhl`` must return ``None`` instead of recursing.
    """
    monkeypatch.chdir(tmp_path)
    assert find_mhl(Path(".")) is None


def test_find_mhl_picks_innermost_nested_legacy_mhl(tmp_path):
    """Legacy flat ``.mhl`` format: when multiple mhls exist in the ancestor chain, the
    directory closest to the file wins, matching the innermost-history semantics used by
    ASC MHL (`MHLHistory.find_history_for_path`)."""
    outer_mhl = tmp_path / "outer_2024-01-01_000000.mhl"
    outer_mhl.write_text("outer")

    inner = tmp_path / "inner"
    inner.mkdir()
    inner_mhl = inner / "inner_2024-02-01_000000.mhl"
    inner_mhl.write_text("inner")

    deeper = inner / "sub"
    deeper.mkdir()

    found = find_mhl(deeper / "clip.mov")
    assert found is not None
    assert found.read_text() == "inner"


def test_find_mhl_picks_latest_when_multiple_in_same_dir(tmp_path):
    """When several mhls live in the same directory, the lexicographically last one wins,
    which matches the convention of date-suffixed filenames (``name_YYYY-MM-DD_HHMMSS.mhl``)
    and therefore selects the newest generation."""
    (tmp_path / "session_2024-01-01_000000.mhl").write_text("old")
    (tmp_path / "session_2024-06-15_120000.mhl").write_text("new")

    found = find_mhl(tmp_path / "clip.mov")
    assert found is not None
    assert found.read_text() == "new"


def test_find_mhl_finds_ancestor_mhl_for_relative_path(tmp_path, monkeypatch):
    """
    CUB-481 (root cause): when the caller passes a relative path (as ``copytree`` does when
    invoked with ``ocopy . /dest``), the mhl search must still consider real filesystem
    ancestors above cwd rather than giving up at ``Path('.')``.
    """
    ancestor_mhl = tmp_path / "ancestor.mhl"
    ancestor_mhl.write_text("ancestor data")

    session = tmp_path / "session"
    session.mkdir()
    (session / "clip.mov").write_bytes(b"")

    monkeypatch.chdir(session)

    found = find_mhl(Path("clip.mov"))
    assert found is not None
    assert found.read_text() == "ancestor data"
