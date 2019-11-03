from queue import Queue
from threading import currentThread
from typing import Optional


def get_progress_queue() -> Optional[Queue]:
    try:
        return currentThread()._progress_queue
    except AttributeError:
        return None
