# o/COPY

[![CI](https://github.com/OTTOMATIC-IO/ocopy/actions/workflows/ci.yml/badge.svg)](https://github.com/OTTOMATIC-IO/ocopy/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/ocopy.svg)](https://pypi.org/project/ocopy/)
[![GitHub license](https://img.shields.io/github/license/OTTOMATIC-IO/ocopy.svg)](https://github.com/OTTOMATIC-IO/ocopy/blob/master/LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![codecov](https://codecov.io/gh/OTTOMATIC-IO/ocopy/branch/master/graph/badge.svg)](https://codecov.io/gh/OTTOMATIC-IO/ocopy)

**o/COPY** copies a directory tree to one or more destinations at once.

- **Hashing.** Each file gets an **xxh64** checksum during the copy (the value recorded in MHL output). **Verification** is on by default: o/COPY re-reads the source and destinations and confirms all xxh64 values match. Disable that with `--dont-verify` or `verify=False`.

- **ASC MHL (default on).** Each destination gets an [**ASC Media Hash List (ASC MHL)**](https://github.com/ascmitc/mhl-specification) history: the **`ascmhl` folder**, **chain file**, and XML **generation** manifests that document checksums together with file metadata, following the layout defined in the spec and read/written by the [`mhllib` / `ascmhl` reference implementation](https://github.com/ascmitc/mhl). o/COPY supplies the xxh64 from the copy step so sealing does not hash file contents again. For flat **`*.mhl`** files in the [original **Media Hash List** format](https://mediahashlist.org) instead, use `--legacy-mhl` or `legacy_mhl=True`. `--no-mhl` / `mhl=False` skips writing MHL output.

- **Skip-existing (default on).** A destination file is fast-skipped only when its size and modification time match the source (within a small tolerance) *and* o/COPY already trusts an xxh64 for that path. Trusted digests are resolved in this order: `.ocopy-checkpoint`, an ASC MHL history in an **`ascmhl` folder**, a legacy flat `*.mhl`, then a `*.xxhash` sidecar. If metadata matches but no trusted hash exists while integrity is required, o/COPY re-reads and verifies so ASC MHL records are never written empty. A destination that exists but disagrees raises unless `--overwrite` / `overwrite=True`.

- **Integrity off.** If both `--no-mhl` and `--dont-verify` are set (or `mhl=False` and `verify=False` in code), only size/mtime are used for skip-existing; hashes are not checked.

- **Resume.** While a run is in progress, each destination tree keeps a `.ocopy-checkpoint` sidecar. When the run finishes without error, those files are removed (including when MHL output is disabled). If you interrupt the CLI, it exits with code `3`, leaves checkpoints in place, and does not append a new ASC MHL generation or other MHL output. Run `ocopy` again to continue and finish.

## Installation / Update

### Recommended: uv

[uv](https://docs.astral.sh/uv/) is an extremely fast Python package and project manager, written in Rust. If you do not have it yet: with [Homebrew](https://brew.sh/), run `brew install uv`; for the official standalone installer and everything else, see [Installing uv](https://docs.astral.sh/uv/getting-started/installation/).

```shell
uv tool install ocopy
```

Update an existing install:

```shell
uv tool upgrade ocopy
```

### With pipx

If you prefer the pip ecosystem, [pipx](https://pipx.pypa.io/) does the same job as `uv tool install`: it puts each CLI app in its own environment and exposes the `ocopy` command on your `PATH`.

```shell
pipx install ocopy
```

```shell
pipx upgrade ocopy
```

## Usage

### CLI
![cli](images/recording.svg)

After install the command is `ocopy`. Pass a **source** directory and one or more **destination** directories (each path must already exist and be a writable folder):

```
ocopy /path/to/source /path/to/dest1 /path/to/dest2
```

Run `ocopy --help` for the full flag list. The introduction above describes skip-existing, verification, ASC MHL histories vs. legacy flat MHL, and checkpoints.

During a long run the CLI tries to keep the system from going to idle sleep; that is best-effort and may not work in headless setups, and o/COPY will warn and continue copying.

### Python

```python
import tempfile
from pathlib import Path
from time import sleep

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

        # ``CopyJob`` starts work as soon as it is constructed.
        job = CopyJob(source, destinations, overwrite=True, verify=True)
        while job.finished is not True:
            sleep(0.1)

        # Print errors
        for error in job.errors:
            print(f"Failed to copy {error.source.name}:\n{error.error_message}")

        # Show the start of the latest ASC MHL generation (XML hash list)
        gen = next((destinations[0] / source.name / "ascmhl").glob("*.mhl"))
        print(gen.read_text()[:800])


if __name__ == "__main__":
    simple_example()
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
