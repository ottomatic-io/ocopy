#!/usr/bin/env python3
import json
import sys
import time
from pathlib import Path

import click

from ocopy.backup_check import get_missing
from ocopy.cli.update import Updater
from ocopy.utils import folder_size, free_space, get_mount
from ocopy.verified_copy import CopyJob


def _report_cancelled(job: CopyJob, machine_readable: bool) -> None:
    """Print the cancel summary (human or JSON) that the CLI exits on.

    Reads ``job.checkpoint_paths`` rather than recomputing from CLI arguments so
    multi-destination runs list every checkpoint and the source of truth stays
    inside ``CopyJob``.
    """
    checkpoints = [str(p.resolve()) for p in job.checkpoint_paths]
    verified = job.verified_files_count
    if machine_readable:
        click.echo(json.dumps({"status": "cancelled", "files_verified": verified, "checkpoints": checkpoints}))
        return
    click.secho(
        f"\nCancelled. {verified} file(s) verified so far; re-run ocopy to resume.",
        fg="yellow",
    )
    for cp in checkpoints:
        click.secho(f"Checkpoint: {cp}", fg="yellow")


@click.command()
@click.version_option(prog_name="o/COPY")
@click.option(
    "--overwrite/--dont-overwrite",
    help="Allow overwriting of destination files (defaults to --dont-overwrite)",
    default=False,
)
@click.option(
    "--verify/--dont-verify",
    help="Verify copy by re-calculating the xxHash of the source and all destinations (defaults to --verify)",
    default=True,
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
    help="Output machine-readable progress (defaults to --human-readable)",
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
@click.argument("source", nargs=1, type=click.Path(exists=True, readable=True, file_okay=False, dir_okay=True))
@click.argument(
    "destinations", nargs=-1, type=click.Path(exists=True, readable=True, writable=True, file_okay=False, dir_okay=True)
)
@click.pass_context
def cli(
    ctx: click.Context,
    overwrite: bool,
    verify: bool,
    skip_existing: bool,
    machine_readable: bool,
    mhl: bool,
    legacy_mhl: bool,
    source: str,
    destinations: list[str],
):
    """
    o/COPY by OTTOMATIC

    Copy SOURCE directory to DESTINATIONS
    """
    # ``--legacy-mhl`` selects the manifest flavor and implies manifest writing. The only
    # contradictory combination is an explicit ``--no-mhl`` together with ``--legacy-mhl``.
    if legacy_mhl:
        if not mhl and ctx.get_parameter_source("mhl") == click.core.ParameterSource.COMMANDLINE:
            raise click.UsageError("--legacy-mhl cannot be combined with --no-mhl")
        mhl = True

    updater = Updater()

    size = folder_size(source)
    for destination in destinations:
        free = free_space(destination)
        if free < size:
            click.secho(
                f"{destination} does not have enough free space (need {size} bytes, have {free} bytes).",
                fg="red",
            )
            sys.exit(1)
    click.secho(f"Copying {source} to {', '.join(destinations)}", fg="green")

    destination_paths = [Path(d) for d in destinations]
    if len(destination_paths) != len({get_mount(d) for d in destination_paths}):
        click.secho("Destinations should all be on different drives.", fg="yellow")

    job = CopyJob(
        Path(source),
        destination_paths,
        overwrite=overwrite,
        verify=verify,
        skip_existing=skip_existing,
        mhl=mhl,
        legacy_mhl=legacy_mhl,
    )

    if machine_readable:
        for _ in job.progress:
            click.echo(job.percent_done)
    else:
        with click.progressbar(job.progress, length=100, item_show_func=lambda name: name) as progress:
            for _ in progress:
                pass

    while not job.finished:
        time.sleep(0.1)
        # TODO: break loop if this takes too long

    if job.interrupted_by_cancel:
        _report_cancelled(job, machine_readable)
        sys.exit(3)

    # TODO: check all destinations in parallel
    for destination in destination_paths:
        missing, _ = get_missing(source, str(destination / Path(source).name))
        if missing:
            missing_list = "\n".join(missing)
            click.secho(
                f"\n{len(missing)} file{'s' if len(missing) > 1 else ''} missing on {destination}:\n{missing_list}",
                fg="red",
            )
            click.secho(
                "This should not happen! Please contact info@ottomatic.io with as much details as possible.", fg="red"
            )

        in_progress_files = list((destination / Path(source).name).glob("**/*copy_in_progress*"))
        if len(in_progress_files):
            in_progress_list = "\n".join([f.as_posix() for f in in_progress_files])
            click.secho(
                f"\n{len(in_progress_files)} file{'s' if len(in_progress_files) > 1 else ''} in progress on "
                f"{destination}:\n{in_progress_list}",
                fg="red",
            )
            click.secho(
                "This should not happen! Please contact info@ottomatic.io with as much details as possible.", fg="red"
            )

    click.echo(f"\n{job.speed / 1000 / 1000:.2f} MB/s")

    if job.skipped_files:
        click.secho(
            f"\nSkipped {job.skipped_files} existing file{'s' if job.skipped_files > 1 else ''} "
            f"with same name, size and modification time.",
            fg="yellow",
        )

    if job.errors:
        for error in job.errors:
            click.secho(f"\nFailed to copy {error.source.name}:\n{error.error_message}", fg="red")

        sys.exit(1)

    if updater.needs_update:
        click.secho("Please update to the latest o/COPY version using `pip3 install -U ocopy`.", fg="blue")

    job.join(timeout=1)
    updater.join(timeout=1)


if __name__ == "__main__":
    cli()  # pragma: no cover
