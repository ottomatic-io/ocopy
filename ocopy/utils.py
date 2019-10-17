import os
import platform
from pathlib import Path
from threading import Thread


def threaded(fn):
    def wrapper(*args, **kwargs):
        t = Thread(target=fn, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()

    return wrapper


def folder_size(path):
    total = 0
    for entry in os.scandir(path):
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += folder_size(entry.path)
    return total


def get_user_display_name() -> str:
    if platform.system() != "Windows":
        import pwd

        return pwd.getpwuid(os.getuid()).pw_gecos
    else:
        import ctypes

        get_user_name_ex = ctypes.windll.secur32.GetUserNameExW
        name_display = 3

        size = ctypes.pointer(ctypes.c_ulong(0))
        get_user_name_ex(name_display, None, size)

        name_buffer = ctypes.create_unicode_buffer(size.contents.value)
        get_user_name_ex(name_display, name_buffer, size)
        return name_buffer.value


def get_mount(path: Path) -> Path:
    # pathlib.Path.is_mount is not implemented on Windows
    while not os.path.ismount(path) and path.parents:
        path = path.parent

    return path
