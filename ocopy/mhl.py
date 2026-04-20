import datetime
import getpass
import os
from _socket import gethostname
from functools import lru_cache
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


def _mhl_text_to_xxh64_index(mhl: str) -> dict[str, str]:
    """Build ``posix_relpath -> xxh64`` from legacy flat MHL XML (first occurrence wins)."""
    if not mhl:
        return {}
    root = ElementTree.fromstring(mhl)
    out: dict[str, str] = {}
    for hash_element in root.findall("hash"):
        file_elem = hash_element.find("file")
        if file_elem is None or not file_elem.text:
            continue
        path_key = file_elem.text
        if path_key in out:
            continue
        xxhash_elem = hash_element.find("xxhash64be")
        if xxhash_elem is not None and xxhash_elem.text:
            out[path_key] = xxhash_elem.text
    return out


@lru_cache(maxsize=128)
def _cached_load_mhl_index(mhl_path_str: str, mtime_ns: int) -> dict[str, str]:
    """Parse legacy flat ``*.mhl`` into a path index; keyed by ``(mhl_path_str, mtime_ns)``.

    ``maxsize=128``: fewer distinct legacy manifest files per process than ASC roots;
    each entry is a ``dict`` of relpath→xxh64. Bounds memory in long-lived processes
    while matching the bounded-LRU pattern used for :func:`ocopy.hash._cached_load_ascmhl`.
    """
    return _mhl_text_to_xxh64_index(Path(mhl_path_str).read_text(encoding="utf-8"))


def xxh64_from_legacy_mhl_path(mhl_path: Path, file_relative_to_mhl_parent: Path) -> str | None:
    """Resolve xxh64 for ``file_relative_to_mhl_parent`` from the legacy ``*.mhl`` at ``mhl_path``."""
    try:
        mtime_ns = mhl_path.stat().st_mtime_ns
    except OSError:
        return None
    index = _cached_load_mhl_index(str(mhl_path.resolve()), mtime_ns)
    return index.get(file_relative_to_mhl_parent.as_posix())


def find_mhl(file_path: Path) -> Path | None:
    """
    Finds the last created mhl which is closest in the folder hierarchy.

    Walks absolute ancestors so a relative ``file_path`` (e.g. ``Path("clip.mov")`` when
    cwd is ``/Volumes/disk/session``) still discovers mhls in real parent directories
    and terminates at the filesystem root or a mount point.
    """
    # ``strict=False`` tolerates non-existent leaves; we only need an absolute path to walk.
    resolved = file_path.resolve()
    for parent in resolved.parents:
        mhl_in_parent = sorted(parent.glob("*.mhl"))
        if mhl_in_parent:
            return mhl_in_parent[-1]
        if os.path.ismount(parent):
            return None
    return None
