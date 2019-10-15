#!/usr/bin/env python3
import shutil
import sys
import time
from pathlib import Path
from typing import List

import click

from ocopy.copy import copy_and_seal
from ocopy.progress import progress_queue
from ocopy.utils import folder_size


@click.command()
@click.version_option(prog_name="o/COPY")
@click.option(
    "--overwrite/--dont-overwrite",
    help="Allow overwriting of destination files (defaults to --dont-overwrite",
    default=False,
)
@click.option(
    "--verify/--dont-verify",
    help="Verify copy by re-calculating the xxHash of the source and all destinations (defaults to --verify)",
    default=True,
)
@click.argument("source", nargs=1, type=click.Path(exists=True, readable=True, file_okay=False, dir_okay=True))
@click.argument(
    "destinations", nargs=-1, type=click.Path(exists=True, readable=True, writable=True, file_okay=False, dir_okay=True)
)
def cli(overwrite: bool, verify: bool, source: str, destinations: List[str]):
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

    length = size
    if verify:
        length *= 2
    with click.progressbar(length=length, item_show_func=lambda name: name) as bar:
        start = time.time()
        copy_and_seal(Path(source), [Path(d) for d in destinations], overwrite=overwrite, verify=verify)

        # Only update the progress bar every 3%
        progress = 0
        step = length / 33

        while True:
            file_path, done = progress_queue.get(timeout=300)
            bar.current_item = Path(file_path).name

            if file_path == "finished":
                bar.finish()
                bar.render_progress()
                break

            progress += done
            if progress >= step:
                bar.update(progress)
                progress = 0

        stop = time.time()
        click.echo(f"\n{size / 1000 / 1000 / (stop - start):.2f} MB/s")


if __name__ == "__main__":
    cli()
