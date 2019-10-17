import platform
from pathlib import Path
from time import sleep

from ocopy.utils import threaded, folder_size, get_user_display_name, get_mount


def test_threaded():
    @threaded
    def forever_running_loop():
        while True:
            sleep(1)

    forever_running_loop()

    # Loop runs in a thread and would block if something was wrong with the threaded wrapper
    assert True


def test_folder_size(tmpdir):
    for i in range(3):
        for j in range(3):
            path = Path(tmpdir) / str(i) / str(j)
            path.mkdir(parents=True)
            for x in range(3):
                (path / str(x)).write_bytes(b"X" * 3)

    assert folder_size(tmpdir) == 3 ** 4


def test_get_user_display_name():
    display_name = get_user_display_name()
    assert isinstance(display_name, str)


def test_get_mount():
    if platform.system() != "Windows":
        assert get_mount(Path("/Some")) == Path("/")
    else:
        assert get_mount(Path("C:/Some")) == Path("C:/")
