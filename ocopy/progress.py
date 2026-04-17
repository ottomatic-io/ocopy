from queue import Queue
from threading import current_thread


def get_progress_queue() -> Queue | None:
    return getattr(current_thread(), "_progress_queue", None)
