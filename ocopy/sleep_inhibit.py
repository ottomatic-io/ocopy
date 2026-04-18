"""Best-effort inhibit of automatic system sleep during long-running work."""

from __future__ import annotations

import warnings
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager

WarnFn = Callable[[str], None] | None


def _default_warn(message: str) -> None:
    # Frame chain: warn -> _default_warn -> sleep_inhibit_best_effort -> contextlib __enter__ -> user ``with``.
    warnings.warn(message, stacklevel=4)


@contextmanager
def sleep_inhibit_best_effort(warn: WarnFn = None) -> Iterator[None]:
    """Enter wakepy ``keep.running`` for the block, or no-op if setup fails.

    Failures (missing session bus, unsupported environment, etc.) are reported
    once via ``warn`` (defaults to :func:`warnings.warn`) and the block still runs.
    """
    warn_fn = warn or _default_warn
    with ExitStack() as stack:
        try:
            from wakepy import keep

            stack.enter_context(keep.running())
        except Exception as exc:  # pragma: no cover - wakepy handles most platforms cleanly
            warn_fn(f"Could not inhibit system sleep ({exc}); continuing copy.")
        yield
