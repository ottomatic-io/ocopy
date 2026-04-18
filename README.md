# o/COPY

[![CI](https://github.com/OTTOMATIC-IO/ocopy/actions/workflows/ci.yml/badge.svg)](https://github.com/OTTOMATIC-IO/ocopy/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/ocopy.svg)](https://pypi.org/project/ocopy/)
[![GitHub license](https://img.shields.io/github/license/OTTOMATIC-IO/ocopy.svg)](https://github.com/OTTOMATIC-IO/ocopy/blob/master/LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![codecov](https://codecov.io/gh/OTTOMATIC-IO/ocopy/branch/master/graph/badge.svg)](https://codecov.io/gh/OTTOMATIC-IO/ocopy)

A multi destination copy tool / library with source and destination verification using xxHash.
By default each completed copy is sealed with **ASC Media Hash List** (an ``ascmhl/`` directory with
``ascmhl_chain.xml`` and generation manifests). Digests are taken from the copy-time xxh64 stream, so sealing does not
re-read file bodies. Use ``--legacy-mhl`` on the CLI (or ``legacy_mhl=True`` on ``CopyJob``) for the older flat MHL
v1.1 ``*.mhl`` output instead of ASC MHL.

**Re-runs, integrity, and cancellation:** With ``--skip-existing`` (the default), o/COPY only fast-skips a file when
size and mtime match the source *and* a trusted xxh64 is already known (ASC MHL, a per-run ``.ocopy-checkpoint``
sidecar, legacy ``.mhl``, or a dot-xxhash sidecar). Otherwise it re-reads source and destinations so manifests never
contain empty digests. Wrong destination bytes raise a verification error unless ``--overwrite`` is set (source wins).
The checkpoint file is written on each destination during a run and deleted after a successful seal; interrupted jobs
can resume cheaply on the next invocation. If you explicitly use ``--no-mhl`` and ``--dont-verify``, metadata match alone
is the contract. A cancelled run does **not** write a manifest; the CLI exits with code ``3`` and points at the
checkpoint path—re-run o/COPY to finish and seal.

## Installation / Update

### With pip
If you have Python 3.11 or newer installed you can just use `pip`:
```
pip3 install -U ocopy
```

## Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management,
[Ruff](https://docs.astral.sh/ruff/) for linting and formatting, and
[ty](https://docs.astral.sh/ty/) for type checking.

```shell
# Create the virtual environment and install runtime + dev dependencies
uv sync

# Run tests
uv run pytest

# Lint, format, and type check
uv run ruff check .
uv run ruff format .
uv run ty check
```

## Usage

### CLI
![cli](images/recording.svg)

During a run, the CLI asks the OS not to enter automatic idle sleep (using [wakepy](https://pypi.org/project/wakepy/)). That is best-effort: in headless or minimal sessions inhibit may be unavailable, in which case o/COPY prints a short warning and continues copying. The `CopyJob` API does not change power settings unless you wrap it yourself (see below).

### Python

```python
import tempfile
from pathlib import Path
from time import sleep

from ocopy.sleep_inhibit import sleep_inhibit_best_effort
from ocopy.verified_copy import CopyJob


def simple_example():
    # For the sake of this example we will create temporary directory.
    # You will not be doing this in your code.
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        # Define source and destination directories
        source = tmp / "source"
        destinations = [tmp / "destination_1", tmp / "destination_2", tmp / "destination_3"]

        # Create some test content
        source.mkdir(parents=True, exist_ok=True)
        (source / "testfile").write_text("Some test content")

        # Create the copy job and wait until it is finished (optional sleep inhibit;
        # wrap construction too because ``CopyJob`` auto-starts background work)
        with sleep_inhibit_best_effort():
            job = CopyJob(source, destinations, overwrite=True, verify=True)
            while job.finished is not True:
                sleep(0.1)

        # Print errors
        for error in job.errors:
            print(f"Failed to copy {error.source.name}:\n{error.error_message}")

        # Show the start of the latest ASC MHL generation manifest
        gen = next((destinations[0] / source.name / "ascmhl").glob("*.mhl"))
        print(gen.read_text()[:800])


if __name__ == "__main__":
    simple_example()
```
