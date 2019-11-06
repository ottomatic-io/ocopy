#!/usr/bin/env python3
import shutil
import sys
import time
from pathlib import Path
from typing import List

import click

from ocopy.copy import CopyJob
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
@click.argument("source", nargs=1, type=click.Path(exists=True, readable=True, file_okay=False, dir_okay=True))
@click.argument(
    "destinations", nargs=-1, type=click.Path(exists=True, readable=True, writable=True, file_okay=False, dir_okay=True)
)
def cli(overwrite: bool, verify: bool, skip_existing: bool, source: str, destinations: List[str]):
    """
    o/COPY by OTTOMATIC

    Copy SOURCE directory to DESTINATIONS
    """
    size = folder_size(source)
    for destination in destinations:
        if shutil.disk_usage(destination).free < size:
            click.secho(f"{destination} does not have enough free space.", fg="red")
            sys.exit(1)
    click.secho(f"Copying {source} to {', '.join(destinations)}", fg="green")

    destinations = [Path(d) for d in destinations]
    if len(destinations) != len({get_mount(d) for d in destinations}):
        click.secho(f"Destinations should all be on different drives.", fg="yellow")

    start = time.time()
    job = CopyJob(Path(source), destinations, overwrite=overwrite, verify=verify, skip_existing=skip_existing)

    with click.progressbar(job.progress, length=100, item_show_func=lambda name: name) as progress:
        for _ in progress:
            pass

    stop = time.time()
    click.echo(f"\n{size / 1000 / 1000 / (stop - start):.2f} MB/s")

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


if __name__ == "__main__":
    cli()  # pragma: no cover
