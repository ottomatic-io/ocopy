import os
import platform
import shutil
import sys
from pathlib import Path
from threading import Thread

from ocopy.ignored import ignored_paths

if sys.platform == "darwin":
    import ctypes
    import ctypes.util

    # macOS `struct statfs` with 64-bit inode layout (from <sys/mount.h>).
    # We call statfs(2) directly because `statvfs(3)` on macOS uses 32-bit block
    # counts and wraps on volumes larger than ~4 TiB (CPython bug fixed in 3.13
    # only, see https://github.com/python/cpython/issues/87804).
    class _StatFs(ctypes.Structure):
        _fields_ = (
            ("f_bsize", ctypes.c_uint32),
            ("f_iosize", ctypes.c_int32),
            ("f_blocks", ctypes.c_uint64),
            ("f_bfree", ctypes.c_uint64),
            ("f_bavail", ctypes.c_uint64),
            ("f_files", ctypes.c_uint64),
            ("f_ffree", ctypes.c_uint64),
            ("f_fsid", ctypes.c_uint64),
            ("f_owner", ctypes.c_uint32),
            ("f_type", ctypes.c_uint32),
            ("f_flags", ctypes.c_uint32),
            ("f_fssubtype", ctypes.c_uint32),
            ("f_fstypename", ctypes.c_char * 16),
            ("f_mntonname", ctypes.c_char * 1024),
            ("f_mntfromname", ctypes.c_char * 1024),
            ("f_reserved", ctypes.c_uint32 * 8),
        )

    _libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    _libc.statfs.argtypes = (ctypes.c_char_p, ctypes.POINTER(_StatFs))
    _libc.statfs.restype = ctypes.c_int

    def _statfs_bavail_bytes(path) -> int:
        fs = _StatFs()
        if _libc.statfs(os.fsencode(os.fspath(path)), ctypes.byref(fs)) != 0:
            errno = ctypes.get_errno()
            raise OSError(errno, os.strerror(errno), os.fspath(path))
        return int(fs.f_bavail) * int(fs.f_bsize)


def threaded(fn):
    def wrapper(*args, **kwargs):
        t = Thread(target=fn, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()

    return wrapper


def folder_size(path):
    total = 0
    all_files = [f for f in Path(path).glob("**/*") if not any(dir_ in f.parts for dir_ in ignored_paths)]
    for entry in all_files:
        if entry.is_file():
            total += entry.stat().st_size
    return total


def get_user_display_name() -> str:
    if platform.system() != "Windows":
        import pwd

        return pwd.getpwuid(os.getuid()).pw_gecos
    else:
        import ctypes

        get_user_name_ex = ctypes.windll.secur32.GetUserNameExW  # ty: ignore[unresolved-attribute]
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


def free_space(path) -> int:
    """Bytes available to a non-superuser on the filesystem containing `path`.

    Works around the macOS `statvfs` 32-bit block-count overflow on Python
    <3.13 by calling `statfs(2)` directly via ctypes. On every other platform
    and on Python 3.13+, defers to `shutil.disk_usage`, whose implementation
    already handles this correctly.
    """
    if sys.platform == "darwin" and sys.version_info < (3, 13):
        return _statfs_bavail_bytes(path)
    return shutil.disk_usage(path).free
