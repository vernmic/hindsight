"""Exclusive runner lock for the tag queue: cron ticks and manual commands
never drain concurrently. Second entrant gets a busy receipt, not a race.

Lock lives beside the queue DB so scratch queues get scratch locks."""
import fcntl
import os
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def runner_lock(queue_db_path):
    path = Path(str(queue_db_path) + ".runner.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_CREAT | os.O_RDWR, 0o666)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
