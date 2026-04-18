import datetime
import getpass
import os
from _socket import gethostname
from pathlib import Path

from defusedxml import ElementTree
from lxml import etree  # ty: ignore[unresolved-import]
from lxml.builder import E

from ocopy.file_info import FileInfo
from ocopy.utils import get_user_display_name


def file_info2mhl_hash(file_info: FileInfo, source: Path):
    now = datetime.datetime.now(datetime.UTC).replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    new_hash = E.hash(
        # FIXME: use path relative to destination instead of source
        E.file(file_info.source.relative_to(source).as_posix()),
        E.size(str(file_info.size)),
        E.lastmodificationdate(
            datetime.datetime.fromtimestamp(file_info.mtime, datetime.UTC)
            .replace(microsecond=0, tzinfo=None)
            .isoformat()
            + "Z"
        ),
        E.xxhash64be(file_info.file_hash),
        E.hashdate(now),
    )
    return new_hash


def create_mhl(start: datetime.datetime):
    start_str = start.replace(microsecond=0).isoformat() + "Z"
    finish = datetime.datetime.now(datetime.UTC).replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    new_mhl = E.hashlist(
        E.creatorinfo(
            E.name(get_user_display_name()),
            E.username(getpass.getuser()),
            E.hostname(gethostname()),
            E.tool("o/COPY"),
            E.startdate(start_str),
            E.finishdate(finish),
        ),
        version="1.1",
    )
    return new_mhl


def write_mhl_to_destinations(new_mhl, destinations: list[Path]):
    for d in destinations:
        timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d_%H%M%S")
        mhl_name = f"{os.path.basename(os.path.abspath(d))}_{timestamp}.mhl"

        with open(d / mhl_name, "wb") as x:
            x.write(etree.tostring(new_mhl, pretty_print=True, encoding="utf-8", xml_declaration=True))


def write_mhl(destinations: list[Path], file_infos: list[FileInfo], source: Path, start: datetime.datetime):
    new_mhl = create_mhl(start)
    for file_info in file_infos:
        new_mhl.append(file_info2mhl_hash(file_info, source))
    write_mhl_to_destinations(new_mhl, destinations)


def get_hash_from_mhl(mhl: str, file_path: Path) -> str | None:
    """
    Reads the xxhash for a file from an mhl string.

    Returns ``None`` if the mhl is empty or does not contain the given file.
    """
    if not mhl:
        return None

    posix_path = file_path.as_posix()
    root = ElementTree.fromstring(mhl)

    for hash_element in root.findall("hash"):
        file_elem = hash_element.find("file")
        if file_elem is not None and file_elem.text == posix_path:
            xxhash_elem = hash_element.find("xxhash64be")
            if xxhash_elem is not None:
                return xxhash_elem.text
    return None


def find_mhl(file_path: Path) -> Path | None:
    """
    Finds the last created mhl which is closest in the folder hierarchy
    """
    if os.path.ismount(file_path):
        return None

    mhl_in_parent = sorted(file_path.parent.glob("*.mhl"))
    if mhl_in_parent:
        return mhl_in_parent[-1]
    else:
        return find_mhl(file_path.parent)
