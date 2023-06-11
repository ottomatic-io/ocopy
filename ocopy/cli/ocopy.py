#!/usr/bin/env python3
import shutil
import sys
import time
from pathlib import Path
from typing import List

import click

from ocopy.backup_check import get_missing
from ocopy.cli.update import Updater
from ocopy.verified_copy import CopyJob
from ocopy.utils import folder_size, get_mount


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
@click.argument("source", nargs=1, type=click.Path(exists=True, readable=True, file_okay=False, dir_okay=True))
@click.argument(
    "destinations", nargs=-1, type=click.Path(exists=True, readable=True, writable=True, file_okay=False, dir_okay=True)
)
def cli(overwrite: bool, verify: bool, skip_existing: bool, machine_readable: bool, source: str, destinations: List[str]):
    """
    o/COPY by OTTOMATIC

    Copy SOURCE directory to DESTINATIONS
    """
    updater = Updater()

    size = folder_size(source)
    for destination in destinations:
        if shutil.disk_usage(destination).free < size:
            click.secho(f"{destination} does not have enough free space.", fg="red")
            sys.exit(1)
    click.secho(f"Copying {source} to {', '.join(destinations)}", fg="green")

    destinations = [Path(d) for d in destinations]
    if len(destinations) != len({get_mount(d) for d in destinations}):
        click.secho("Destinations should all be on different drives.", fg="yellow")

    job = CopyJob(Path(source), destinations, overwrite=overwrite, verify=verify, skip_existing=skip_existing)

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

    # TODO: check all destinations in parallel
    for destination in destinations:
        missing, _ = get_missing(source, destination / Path(source).name)
        if missing:
            missing_list = "\n".join(missing)
            click.secho(
                f"\n{len(missing)} file{'s' if len(missing) > 1 else ''} missing on {destination}:\n{missing_list}",
                fg="red",
            )
            click.secho(
                f"This should not happen! Please contact info@ottomatic.io with as much details as possible.", fg="red"
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
                f"This should not happen! Please contact info@ottomatic.io with as much details as possible.", fg="red"
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
        click.secho(f"Please update to the latest o/COPY version using `pip3 install -U ocopy`.", fg="blue")

    job.join(timeout=1)
    updater.join(timeout=1)


if __name__ == "__main__":
    cli()  # pragma: no cover
