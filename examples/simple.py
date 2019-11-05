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
