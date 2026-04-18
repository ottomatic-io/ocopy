"""Seal ASC MHL histories using mhllib with precomputed xxh64 digests (no media re-read)."""

from __future__ import annotations

import datetime
import importlib.metadata
import os
import platform
from pathlib import Path

from ascmhl import errors
from ascmhl import utils as ascmhl_utils
from ascmhl.__version__ import ascmhl_chainfile_name, ascmhl_folder_name
from ascmhl.chain import MHLChain
from ascmhl.commands import get_ignore_spec_including_nested_ignores, test_for_missing_files
from ascmhl.generator import MHLGenerationCreationSession
from ascmhl.hashlist import MHLAuthor, MHLCreatorInfo, MHLProcess, MHLProcessInfo, MHLTool
from ascmhl.history import MHLHistory
from ascmhl.traverse import post_order_lexicographic

from ocopy.file_info import FileInfo
from ocopy.ignored import ignored_paths
from ocopy.utils import get_user_display_name


class ASCMHLSealError(Exception):
    """Raised when ASC MHL sealing fails (completeness, verification, or chain errors)."""


def _commit_ocopy_generation(session: MHLGenerationCreationSession) -> None:
    creator_info = MHLCreatorInfo()
    creator_info.tool = MHLTool("o/COPY", importlib.metadata.version("ocopy"))
    creator_info.creation_date = ascmhl_utils.datetime_now_isostring()
    creator_info.host_name = platform.node()
    creator_info.authors.append(MHLAuthor(get_user_display_name()))

    process_info = MHLProcessInfo()
    process_info.process = MHLProcess("in-place")

    session.commit(creator_info, process_info)


def _bootstrap_history(content_root: str) -> MHLHistory:
    history = MHLHistory()
    asc_folder = os.path.join(content_root, ascmhl_folder_name)
    chain_path = os.path.join(asc_folder, ascmhl_chainfile_name)
    history.asc_mhl_path = asc_folder
    history.chain = MHLChain(chain_path)
    history.hash_lists = []
    return history


def _load_or_bootstrap_history(content_root: str) -> MHLHistory:
    asc_dir = os.path.join(content_root, ascmhl_folder_name)
    if os.path.isdir(asc_dir):
        try:
            return MHLHistory.load_from_path(content_root)
        except errors.NoMHLChainException as e:
            raise ASCMHLSealError(f"Invalid ASC MHL folder (missing chain): {e}") from e
    return _bootstrap_history(content_root)


def _file_infos_by_relposix(file_infos: list[FileInfo], source_root: Path) -> dict[str, FileInfo]:
    """Index ``file_infos`` by their POSIX path relative to ``source_root``.

    Loudly rejects contract violations that would otherwise silently lose a file
    from the seal: sources outside ``source_root`` or two ``FileInfo`` entries
    collapsing onto the same relative path.
    """
    root = source_root.resolve()
    by_rel: dict[str, FileInfo] = {}
    for fi in file_infos:
        resolved = fi.source.resolve()
        if not resolved.is_relative_to(root):
            raise ASCMHLSealError(f"FileInfo source {fi.source} is outside source_root {source_root}")
        rel = resolved.relative_to(root).as_posix()
        existing = by_rel.get(rel)
        if existing is not None:
            raise ASCMHLSealError(f"duplicate relative path {rel!r} for sources {existing.source} and {fi.source}")
        by_rel[rel] = fi
    return by_rel


def seal_ascmhl_at_destination(content_root: Path, source_root: Path, file_infos: list[FileInfo]) -> None:
    """
    Append one ASC MHL generation under ``content_root`` using hashes from ``file_infos``.

    Directory content/structure hashes are omitted (``ascmhl create -n`` parity) to avoid
    extra implementation surface; per-file records still match ocopy's xxh64.
    """
    root = str(Path(content_root).resolve())
    src_root = source_root.resolve()
    root_path = Path(root)
    by_rel = _file_infos_by_relposix(file_infos, src_root)

    history = _load_or_bootstrap_history(root)
    ignore_spec = get_ignore_spec_including_nested_ignores(history, tuple(sorted(ignored_paths)), None)
    session = MHLGenerationCreationSession(history, ignore_spec)

    # Tracks files present in prior generations; anything still here at the end is "missing"
    # on disk and will surface via ``test_for_missing_files``.
    not_found_paths = history.set_of_file_paths()
    empty_dir_hashes: dict[str, str] = {}

    for folder_path, children in post_order_lexicographic(root, ignore_spec.get_path_spec()):
        for item_name, is_dir in children:
            if is_dir:
                continue
            file_path = os.path.join(folder_path, item_name)
            not_found_paths.discard(file_path)

            rel_posix = Path(file_path).resolve().relative_to(root_path).as_posix()
            fi = by_rel.get(rel_posix)
            if fi is None:
                # File was ignored by copytree (e.g. ``.DS_Store``) or lives outside the copy set.
                continue

            mtime = datetime.datetime.fromtimestamp(fi.mtime)
            if not session.append_file_hash(file_path, fi.size, mtime, "xxh64", fi.file_hash):
                raise ASCMHLSealError(f"ASC MHL hash mismatch while sealing {rel_posix}")

        # ``--no_directory_hashes`` parity: record directory entries without content/structure hashes.
        modification_date = datetime.datetime.fromtimestamp(os.path.getmtime(folder_path))
        session.append_multiple_format_directory_hashes(
            folder_path, modification_date, empty_dir_hashes, empty_dir_hashes
        )

    _commit_ocopy_generation(session)

    exc = test_for_missing_files(not_found_paths, root, ignore_spec)
    if exc is not None:
        raise ASCMHLSealError(f"ASC MHL completeness check failed: {exc}") from exc


def seal_ascmhl_destinations(destinations: list[Path], source: Path, file_infos: list[FileInfo]) -> None:
    for dest_root in destinations:
        try:
            seal_ascmhl_at_destination(dest_root, source, file_infos)
        except ASCMHLSealError as e:
            raise ASCMHLSealError(f"failed sealing destination {dest_root}: {e}") from e
        except (
            errors.CompletenessCheckFailedException,
            errors.VerificationFailedException,
            errors.NoMHLHistoryException,
            errors.ModifiedMHLManifestFileException,
            errors.MissingMHLManifestException,
            errors.NoMHLChainException,
            AssertionError,
        ) as e:
            raise ASCMHLSealError(f"failed sealing destination {dest_root}: {e}") from e
