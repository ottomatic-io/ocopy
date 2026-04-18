"""XSD checks and ``ascmhl-debug verify`` for sealed trees (schemas vendored under ``fixtures/``)."""

from __future__ import annotations

import subprocess
from pathlib import Path

from lxml import etree  # ty: ignore[unresolved-import]  # lxml ships no type stubs

_XSD_DIR = Path(__file__).resolve().parent / "fixtures" / "ascmhl_xsd"


def _assert_xsd(schema, xml_path: Path) -> None:
    doc = etree.parse(str(xml_path))
    if not schema.validate(doc):
        raise AssertionError(f"XSD validation failed for {xml_path}:\n{schema.error_log}")


def validate_ascmhl_xsd(content_root: Path) -> None:
    """Validate ``ascmhl_chain.xml`` and every ``*.mhl`` manifest against the bundled ASCMHL XSD.

    Raises ``AssertionError`` on any schema violation so CI (and pytest) fails loudly,
    matching the contract of ``ascmhl-debug xsd-schema-check``.
    """
    ascmhl = content_root / "ascmhl"
    if not ascmhl.is_dir():
        raise AssertionError(f"expected ascmhl/ under {content_root}")

    mhl_xsd = etree.XMLSchema(etree.parse(str(_XSD_DIR / "ASCMHL.xsd")))
    chain_xsd = etree.XMLSchema(etree.parse(str(_XSD_DIR / "ASCMHLDirectory__combined.xsd")))

    _assert_xsd(chain_xsd, ascmhl / "ascmhl_chain.xml")

    manifests = sorted(ascmhl.glob("*.mhl"))
    if not manifests:
        raise AssertionError(f"no generation manifests under {ascmhl}")
    for mhl in manifests:
        _assert_xsd(mhl_xsd, mhl)


def run_ascmhl_debug_verify(content_root: Path) -> None:
    """Run ``ascmhl-debug verify`` against ``content_root``; surface captured output on failure.

    ``ascmhl-debug`` ships with the hard dependency ``ascmhl>=1.2`` so the binary is
    always present in a correctly provisioned environment. We capture its output to
    keep passing runs quiet, then re-raise with both streams attached so pytest does
    not hide why a semantic verification failed.
    """
    try:
        subprocess.run(
            ["ascmhl-debug", "verify", str(content_root)],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as err:
        raise AssertionError(
            f"ascmhl-debug verify failed for {content_root} (exit {err.returncode}):\n"
            f"--- stdout ---\n{err.stdout}\n--- stderr ---\n{err.stderr}"
        ) from err
