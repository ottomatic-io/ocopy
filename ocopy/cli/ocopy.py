#!/usr/bin/env python3
import math
import sys
import time
from pathlib import Path

import click

from ocopy.backup_check import get_missing
from ocopy.cli.jsonl_emitter import JSONLEmitter
from ocopy.cli.update import Updater, suggested_update_command
from ocopy.hash import DEFAULT_ALGORITHM, SUPPORTED_ALGORITHMS
from ocopy.sleep_inhibit import sleep_inhibit_best_effort
from ocopy.utils import folder_size, free_space, get_mount
from ocopy.verified_copy import CopyJob, destination_roots


@click.command()
@click.version_option(prog_name="o/COPY", package_name="ocopy")
@click.option(
    "--overwrite/--dont-overwrite",
    help="Allow overwriting of destination files (defaults to --dont-overwrite)",
    default=False,
)
@click.option(
    "--verify/--dont-verify",
    help="Verify copy by re-calculating the hash of the source and all destinations (defaults to --verify)",
    default=True,
)
@click.option(
    "--hash-algorithm",
    type=click.Choice(list(SUPPORTED_ALGORITHMS)),
    default=DEFAULT_ALGORITHM,
    show_default=True,
    help="Hash algorithm used for verification and manifests.",
)
@click.option(
    "--skip-existing/--dont-skip",
    help=(
        "Skip existing files if they have the same size and modification time "
        "as the source (defaults to --skip-existing)"
    ),
    default=True,
)
@click.option(
    "--machine-readable/--human-readable",
    help=(
        "Emit a JSON Lines (JSONL) event stream on stdout instead of the human progress bar. "
        "See docs/machine-readable.md for the event reference. Defaults to --human-readable."
    ),
    default=False,
)
@click.option(
    "--mhl/--no-mhl",
    help=(
        "Write ASC Media Hash List (``ascmhl/`` chain + generation manifests) to each destination (defaults to --mhl)"
    ),
    default=True,
)
@click.option(
    "--legacy-mhl",
    is_flag=True,
    default=False,
    help="Write legacy flat MHL v1.1 ``*.mhl`` files instead of ASC MHL ``ascmhl/`` (implies --mhl)",
)
@click.option(
    "--contents",
    is_flag=True,
    default=False,
    help=(
        "Copy the contents of SOURCE directly into each destination instead of creating a folder "
        "named after SOURCE inside it (the equivalent of a trailing slash in rsync or cp). "
        "MHL output and checkpoints are then written to the destination itself."
    ),
)
@click.argument("source", nargs=1, type=click.Path(exists=True, readable=True, file_okay=False, dir_okay=True))
@click.argument(
    "destinations", nargs=-1, type=click.Path(exists=True, readable=True, writable=True, file_okay=False, dir_okay=True)
)
@click.pass_context
def cli(
    ctx: click.Context,
    overwrite: bool,
    verify: bool,
    hash_algorithm: str,
    skip_existing: bool,
    machine_readable: bool,
    mhl: bool,
    legacy_mhl: bool,
    contents: bool,
    source: str,
    destinations: list[str],
):
    """
    o/COPY by OTTOMATIC

    Copy SOURCE directory to DESTINATIONS

    By default each destination gets a folder named after SOURCE. With --contents
    the files of SOURCE are copied straight into each destination.
    """
    # ``--legacy-mhl`` selects the manifest flavor and implies manifest writing. The only
    # contradictory combination is an explicit ``--no-mhl`` together with ``--legacy-mhl``.
    if legacy_mhl:
        if not mhl and ctx.get_parameter_source("mhl") == click.core.ParameterSource.COMMANDLINE:
            raise click.UsageError("--legacy-mhl cannot be combined with --no-mhl")
        if hash_algorithm != "xxh64":
            raise click.UsageError("--legacy-mhl only supports --hash-algorithm xxh64")
        mhl = True

    emitter: JSONLEmitter | None = JSONLEmitter() if machine_readable else None
    updater = Updater()

    source_path = Path(source)
    destination_paths = [Path(d) for d in destinations]
    dest_roots = destination_roots(source_path, destination_paths, contents)

    size = folder_size(source)
    for destination in destinations:
        free = free_space(destination)
        if free < size:
            if emitter:
                emitter.result_error(
                    code="insufficient_space",
                    errors=[
                        {
                            "destination": str(Path(destination).resolve()),
                            "needed_bytes": size,
                            "available_bytes": free,
                            "message": f"{destination} does not have enough free space.",
                        }
                    ],
                )
            else:
                click.secho(
                    f"{destination} does not have enough free space (need {size} bytes, have {free} bytes).",
                    fg="red",
                )
            sys.exit(1)

    if emitter:
        emitter.start(
            source=source_path,
            destinations=destination_paths,
            hash_algorithm=hash_algorithm,
            verify=verify,
            skip_existing=skip_existing,
            mhl=mhl,
            legacy_mhl=legacy_mhl,
            contents=contents,
            total_bytes=size,
        )
    elif contents:
        click.secho(f"Copying contents of {source} into {', '.join(destinations)}", fg="green")
    else:
        click.secho(f"Copying {source} to {', '.join(destinations)}", fg="green")

    if len(destination_paths) != len({get_mount(d) for d in destination_paths}):
        if emitter:
            emitter.warning(code="same_drive", message="Destinations should all be on different drives.")
        else:
            click.secho("Destinations should all be on different drives.", fg="yellow")

    def _sleep_inhibit_warn(msg: str) -> None:
        if emitter:
            emitter.warning(code="sleep_inhibit_unavailable", message=msg)
        else:
            click.secho(msg, fg="yellow")

    with sleep_inhibit_best_effort(warn=_sleep_inhibit_warn):
        job = CopyJob(
            source_path,
            destination_paths,
            overwrite=overwrite,
            verify=verify,
            skip_existing=skip_existing,
            mhl=mhl,
            legacy_mhl=legacy_mhl,
            algorithm=hash_algorithm,
            contents=contents,
        )
        if emitter:
            for _ in job.progress:
                emitter.progress(
                    percent=job.percent_done,
                    phase=job.current_phase,
                    current_file=job.current_item,
                    speed_bytes_per_sec=job.live_speed,
                    eta_seconds=job.eta_seconds,
                )
        else:

            def _fmt_eta(seconds: float) -> str:
                if not math.isfinite(seconds) or seconds < 0:
                    return "--:--"
                s = int(seconds)
                return f"{s // 60:02d}:{s % 60:02d}"

            with click.progressbar(
                job.progress,
                length=100,
                show_eta=False,
                item_show_func=lambda name: (
                    f"{name or ''} — {job.live_speed / 1_000_000:.1f} MB/s — ETA {_fmt_eta(job.eta_seconds)}"
                ),
            ) as progress:
                for _ in progress:
                    pass

        while not job.finished:
            time.sleep(0.1)
            # TODO: break loop if this takes too long

        if job.interrupted_by_cancel:
            if emitter:
                emitter.result_cancelled(
                    files_verified=job.verified_files_count,
                    checkpoints=job.checkpoint_paths,
                )
            else:
                click.secho(
                    f"\nCancelled. {job.verified_files_count} file(s) verified so far; re-run ocopy to resume.",
                    fg="yellow",
                )
                for cp in job.checkpoint_paths:
                    click.secho(f"Checkpoint: {cp.resolve()}", fg="yellow")
            sys.exit(3)

        # TODO: check all destinations in parallel
        for destination, dest_root in zip(destination_paths, dest_roots, strict=True):
            missing, _ = get_missing(source, str(dest_root))
            if missing:
                if emitter:
                    emitter.warning(
                        code="missing_files",
                        message=(
                            f"{len(missing)} file{'s' if len(missing) > 1 else ''} missing on {destination}. "
                            "This should not happen! Please contact info@ottomatic.io."
                        ),
                        destination=str(destination.resolve()),
                        paths=list(missing),
                    )
                else:
                    missing_list = "\n".join(missing)
                    click.secho(
                        f"\n{len(missing)} file{'s' if len(missing) > 1 else ''} missing on "
                        f"{destination}:\n{missing_list}",
                        fg="red",
                    )
                    click.secho(
                        "This should not happen! Please contact info@ottomatic.io with as much details as possible.",
                        fg="red",
                    )

            in_progress_files = list(dest_root.glob("**/*copy_in_progress*"))
            if in_progress_files:
                if emitter:
                    emitter.warning(
                        code="in_progress_leftover",
                        message=(
                            f"{len(in_progress_files)} file{'s' if len(in_progress_files) > 1 else ''} "
                            f"in progress on {destination}. This should not happen! "
                            "Please contact info@ottomatic.io."
                        ),
                        destination=str(destination.resolve()),
                        paths=[f.as_posix() for f in in_progress_files],
                    )
                else:
                    in_progress_list = "\n".join([f.as_posix() for f in in_progress_files])
                    click.secho(
                        f"\n{len(in_progress_files)} file{'s' if len(in_progress_files) > 1 else ''} in progress on "
                        f"{destination}:\n{in_progress_list}",
                        fg="red",
                    )
                    click.secho(
                        "This should not happen! Please contact info@ottomatic.io with as much details as possible.",
                        fg="red",
                    )

        if job.errors:
            if emitter:
                emitter.result_error(
                    code="copy_failed",
                    errors=[
                        {
                            "source": str(err.source),
                            "destinations": [str(d) for d in err.destinations],
                            "message": err.error_message,
                        }
                        for err in job.errors
                    ],
                    files_copied=job.verified_files_count,
                    files_skipped=job.skipped_files,
                )
            else:
                click.echo(f"\n{job.speed / 1000 / 1000:.2f} MB/s")
                if job.skipped_files:
                    click.secho(
                        f"\nSkipped {job.skipped_files} existing file{'s' if job.skipped_files > 1 else ''} "
                        f"with same name, size and modification time.",
                        fg="yellow",
                    )
                for error in job.errors:
                    click.secho(f"\nFailed to copy {error.source.name}:\n{error.error_message}", fg="red")
            sys.exit(1)

        if updater.needs_update:
            cmd = suggested_update_command()
            if emitter:
                emitter.warning(
                    code="update_available",
                    message=f"Please update to the latest o/COPY version using `{cmd}`.",
                    command=cmd,
                )
            # The human-readable update notice fires after the speed/skipped lines below.

        if emitter:
            emitter.result_ok(
                files_copied=job.verified_files_count,
                files_skipped=job.skipped_files,
                bytes_copied=size,
                duration_s=job.elapsed,
                speed_bytes_per_sec=job.speed,
                destinations=destination_paths,
                hash_algorithm=hash_algorithm,
            )
        else:
            click.echo(f"\n{job.speed / 1000 / 1000:.2f} MB/s")
            if job.skipped_files:
                click.secho(
                    f"\nSkipped {job.skipped_files} existing file{'s' if job.skipped_files > 1 else ''} "
                    f"with same name, size and modification time.",
                    fg="yellow",
                )
            if updater.needs_update:
                cmd = suggested_update_command()
                click.secho(f"Please update to the latest o/COPY version using `{cmd}`.", fg="blue")

        job.join(timeout=1)
    updater.join(timeout=1)


if __name__ == "__main__":
    cli()  # pragma: no cover
