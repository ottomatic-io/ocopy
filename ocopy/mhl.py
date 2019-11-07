import datetime
import getpass
import os
from _socket import gethostname
from pathlib import Path
from typing import List, Optional

from defusedxml import ElementTree
from lxml import etree
from lxml.builder import E

from ocopy.file_info import FileInfo
from ocopy.utils import get_user_display_name


def file_info2mhl_hash(file_info: FileInfo, source: Path):
    now = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    new_hash = E.hash(
        # FIXME: use path relative to destination instead of source
        E.file(file_info.source.relative_to(source).as_posix()),
        E.size(str(file_info.size)),
        E.xxhash64be(file_info.file_hash),
        E.lastmodificationdate(
            datetime.datetime.utcfromtimestamp(file_info.mtime).replace(microsecond=0).isoformat() + "Z"
        ),
        E.hashdate(now),
    )
    return new_hash


def create_mhl(start: datetime):
    start = start.replace(microsecond=0).isoformat() + "Z"
    finish = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    new_mhl = E.hashlist(
        E.creatorinfo(
            E.name(get_user_display_name()),
            E.username(getpass.getuser()),
            E.hostname(gethostname()),
            E.tool("o/COPY"),
            E.startdate(start),
            E.finishdate(finish),
        ),
        version="1.1",
    )
    return new_mhl


def write_mhl_to_destinations(new_mhl, destinations: List[Path]):
    for d in destinations:
        mhl_name = (
            f"{os.path.basename(os.path.abspath(d))}_{datetime.datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}.mhl"
        )

        with open(d / mhl_name, "wb") as x:
            x.write(etree.tostring(new_mhl, pretty_print=True, encoding="utf-8", xml_declaration=True))


def write_mhl(destinations: List[Path], file_infos: List[FileInfo], source: Path, start: datetime):
    new_mhl = create_mhl(start)
    for file_info in file_infos:
        new_mhl.append(file_info2mhl_hash(file_info, source))
    write_mhl_to_destinations(new_mhl, destinations)


def get_hash_from_mhl(mhl: str, file_path: Path) -> str:
    """
    Reads the xxhash for a file from an mhl string
    """
    if not mhl:
        return ""

    posix_path = file_path.as_posix()
    root = ElementTree.fromstring(mhl)

    for hash_element in root.findall("hash"):
        if hash_element.find("file").text == posix_path:
            return hash_element.find("xxhash64be").text


def find_mhl(file_path: Path) -> Optional[Path]:
    """
    Finds the last created mhl which is closest in the folder hierarchy
    """
    if os.path.ismount(file_path.as_posix()):
        return

    mhl_in_parent = sorted(list(file_path.parent.glob("*.mhl")))
    if mhl_in_parent:
        return mhl_in_parent[-1]
    else:
        return find_mhl(file_path.parent)
