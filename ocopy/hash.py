from concurrent import futures
from functools import partial
from pathlib import Path
from queue import Queue

import xxhash
from ascmhl import errors as ascmhl_errors
from ascmhl.__version__ import ascmhl_folder_name
from ascmhl.history import MHLHistory

from ocopy.checkpoint import Checkpoint
from ocopy.dot_hash import find_dot_xxhash, get_hash_from_dot_xxhash
from ocopy.mhl import find_mhl, get_hash_from_mhl
from ocopy.progress import get_progress_queue


def get_hash(file_path: Path, progress_queue: Queue | None = None, total_files: int = 1) -> str:
    x = xxhash.xxh64()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            x.update(chunk)
            if progress_queue:
                progress_queue.put(
                    (
                        file_path.with_name(file_path.name.replace(".copy_in_progress", "") + " (verify)"),
                        len(chunk) / total_files,
                    )
                )

    return x.hexdigest()


def multi_xxhash_check(filenames: list[Path]) -> str:
    with futures.ThreadPoolExecutor(max_workers=len(filenames)) as executor:
        unique_file_hashes = {
            file_hash
            for file_hash in executor.map(
                partial(get_hash, progress_queue=get_progress_queue(), total_files=len(filenames)), filenames
            )
        }

    return unique_file_hashes.pop() if len(unique_file_hashes) == 1 else "hashes_do_not_match"


def _innermost_root_with_marker(file_path: Path, marker: str, *, is_dir: bool) -> Path | None:
    """Walk from ``file_path`` upwards and return the deepest ancestor containing ``marker``.

    ``marker`` is checked against ``parent / marker`` with ``is_dir`` distinguishing
    a directory marker (e.g. ``ascmhl``) from a file marker (e.g. ``.ocopy-checkpoint``).
    Returns ``None`` when no ancestor qualifies or when ``file_path`` cannot be
    resolved as relative to any candidate (e.g. symlink crossings).
    """
    p = file_path.resolve()
    candidates: list[Path] = []
    if p.is_dir():
        candidates.append(p)
    candidates.extend(p.parents)
    best: Path | None = None
    for parent in candidates:
        marker_path = parent / marker
        if is_dir:
            if not marker_path.is_dir():
                continue
        elif not marker_path.is_file():
            continue
        try:
            p.relative_to(parent)
        except ValueError:
            continue
        if best is None or len(parent.parts) > len(best.parts):
            best = parent
    return best


def _xxh64_from_checkpoint(content_root: Path, file_path: Path) -> str | None:
    try:
        rel = file_path.resolve().relative_to(content_root.resolve()).as_posix()
    except ValueError:
        return None
    try:
        st = file_path.stat()
    except OSError:
        return None
    return Checkpoint(content_root).lookup(rel, st.st_size, st.st_mtime)


def _xxh64_latest_from_ascmhl(content_root: Path, file_path: Path) -> str | None:
    try:
        history = MHLHistory.load_from_path(str(content_root))
    except (
        ascmhl_errors.NoMHLChainException,
        ascmhl_errors.ModifiedMHLManifestFileException,
        ascmhl_errors.MissingMHLManifestException,
        OSError,
    ):
        return None

    try:
        rel = file_path.resolve().relative_to(content_root.resolve())
    except ValueError:
        return None

    # ``media_hashes_path_map`` keys are produced via ``convert_posix_to_local_path``
    # in the ASC MHL XML parser, which calls ``PureWindowsPath`` on Windows - so the
    # stored key has backslash separators there. Using ``str(rel)`` gives the native
    # separator on each OS and matches the library's convention; ``.as_posix()``
    # would miss on Windows. We try the POSIX form as a fallback for histories that
    # were written on a POSIX host and then consulted on Windows (same filesystem,
    # cross-platform read).
    for hash_list in reversed(history.hash_lists):
        for form in (str(rel), rel.as_posix()):
            media_hash = hash_list.find_media_hash_for_path(form)
            if media_hash is None or media_hash.is_directory:
                continue
            entry = media_hash.find_hash_entry_for_format("xxh64")
            if entry is not None and entry.hash_string:
                return entry.hash_string
    return None


def find_hash(file_path: Path) -> str | None:
    ck_root = _innermost_root_with_marker(file_path, Checkpoint.FILENAME, is_dir=False)
    if ck_root is not None:
        ck_hash = _xxh64_from_checkpoint(ck_root, file_path)
        if ck_hash:
            return ck_hash

    asc_root = _innermost_root_with_marker(file_path, ascmhl_folder_name, is_dir=True)
    if asc_root is not None:
        asc_hash = _xxh64_latest_from_ascmhl(asc_root, file_path)
        if asc_hash:
            return asc_hash

    dot_mhl = find_mhl(file_path)
    if dot_mhl:
        file_hash = get_hash_from_mhl(dot_mhl.read_text(), file_path.relative_to(dot_mhl.parent))
        if file_hash:
            return file_hash

    dot_xxhash = find_dot_xxhash(file_path)
    if dot_xxhash:
        file_hash = get_hash_from_dot_xxhash(dot_xxhash.read_text(), file_path.relative_to(dot_xxhash.parent))
        if file_hash:
            return file_hash

    return None
