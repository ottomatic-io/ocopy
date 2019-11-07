from pathlib import Path

from ocopy.mhl import get_hash_from_mhl, find_mhl


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
    mhl_path = (tmpdir / "some.mhl")
    mhl_path.write_text("some data")

    file_dir = tmpdir / "some" / "sub" / "dir"
    file_dir.mkdir(parents=True)

    assert find_mhl(file_dir / "some_file.mov").read_text() == "some data"

