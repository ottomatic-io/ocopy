# o/COPY

[![CI](https://github.com/OTTOMATIC-IO/ocopy/actions/workflows/ci.yml/badge.svg)](https://github.com/OTTOMATIC-IO/ocopy/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/ocopy.svg)](https://pypi.org/project/ocopy/)
[![GitHub license](https://img.shields.io/github/license/OTTOMATIC-IO/ocopy.svg)](https://github.com/OTTOMATIC-IO/ocopy/blob/master/LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![codecov](https://codecov.io/gh/OTTOMATIC-IO/ocopy/branch/master/graph/badge.svg)](https://codecov.io/gh/OTTOMATIC-IO/ocopy)

A multi destination copy tool / library with source and destination verification using xxHash.

## Installation / Update

### With pip
If you have Python 3.10 or newer installed you can just use `pip`:
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

        # Create the copy job and wait until it is finished
        job = CopyJob(source, destinations, overwrite=True, verify=True)
        while job.finished is not True:
            sleep(0.1)

        # Print errors
        for error in job.errors:
            print(f"Failed to copy {error.source.name}:\n{error.error_message}")

        # Show content of the mhl file
        mhl_file_content = list(destinations[0].glob("**/*.mhl"))[0].read_text()
        print(mhl_file_content)


if __name__ == "__main__":
    simple_example()
```
