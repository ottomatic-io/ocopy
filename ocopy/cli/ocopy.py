#!/usr/bin/env python3
import time
from pathlib import Path
from typing import List

import click

from ocopy.copy import copy_and_seal
from ocopy.progress import progress_queue
from ocopy.utils import folder_size


@click.command()
@click.option(
    "--overwrite/--dont-overwrite",
    help="Allow overwriting of destination files (defaults to --dont-overwrite",
    default=False,
)
@click.argument(
    "source",
    nargs=1,
    type=click.Path(exists=True, readable=True, file_okay=False, dir_okay=True),
)
@click.argument(
    "destinations",
    nargs=-1,
    type=click.Path(
        exists=True, readable=True, writable=True, file_okay=False, dir_okay=True
    ),
)
def cli(overwrite: bool, source: str, destinations: List[str]):
    """
    Copy SOURCE (file or directory) to DESTINATIONS
    """
    size = folder_size(source)
    total_done = 0
    click.echo(f"Copying {source} to {', '.join(destinations)}")

    with click.progressbar(length=size * 2, item_show_func=lambda name: name) as bar:
        start = time.time()
        # file_infos = copytree(source, destinations, overwrite=overwrite)
        copy_and_seal(Path(source), [Path(d) for d in destinations], overwrite=overwrite)

        while True:
            file_path, done = progress_queue.get(timeout=300)
            if file_path == 'finished':
                bar.finish()
                bar.render_progress()
                break

            bar.current_item = Path(file_path).name
            bar.update(done)
            total_done += done

        stop = time.time()

        print(f"\n{size / 1000 / 1000 / (stop - start):.2f} MB/s")


if __name__ == "__main__":
    # TODO: - skip existing
    #       - progress reporting
    #       -
    cli()
