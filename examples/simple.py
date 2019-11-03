from pathlib import Path
from time import sleep

from ocopy.copy import CopyJob


def simple_example():
    # Define source and destination directories
    source = Path("/tmp/source")
    destinations = [Path("/tmp/destination_1"), Path("/tmp/destination_2"), Path("/tmp/destination_3")]

    # Create some test content
    source.mkdir(parents=True, exist_ok=True)
    (source / "testfile").write_text("Some test content")

    # Create the copy job and wait until it is finished
    job = CopyJob(source, destinations, overwrite=True, verify=True)
    while job.finished is not True:
        sleep(0.1)

    # Show content of the mhl file
    mhl_file_content = list(destinations[0].glob("**/*.mhl"))[0].read_text()
    print(mhl_file_content)


if __name__ == "__main__":
    simple_example()
