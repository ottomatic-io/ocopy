# o/COPY

[![PyPI version](https://travis-ci.org/OTTOMATIC-IO/ocopy.svg?branch=master)](https://travis-ci.org/OTTOMATIC-IO/ocopy)
[![PyPI version](https://badge.fury.io/py/ocopy.svg)](https://pypi.org/project/ocopy/)
[![GitHub license](https://img.shields.io/github/license/OTTOMATIC-IO/ocopy.svg)](https://github.com/OTTOMATIC-IO/ocopy/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![codecov](https://codecov.io/gh/OTTOMATIC-IO/ocopy/branch/master/graph/badge.svg)](https://codecov.io/gh/OTTOMATIC-IO/ocopy)

A multi destination copy tool / library with source and destination verification using xxHash.

## Installation / Update

### With pip
If you have Python 3 installed you can just use `pip`:
```
pip3 install -U ocopy
```

## Usage

### CLI
![cli](images/recording.svg)

### Python

```python
import tempfile
from pathlib import Path
from time import sleep

from ocopy.copy import CopyJob


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
