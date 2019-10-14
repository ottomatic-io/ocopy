import datetime
import getpass
import os
import pwd
from _socket import gethostname
from pathlib import Path
from typing import List

from lxml import etree
from lxml.builder import E
from ocopy.file_info import FileInfo


def file_info2mhl_hash(file_info: FileInfo, source: Path):
    now = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    new_hash = E.hash(
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
            E.name(pwd.getpwuid(os.getuid()).pw_gecos),
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
