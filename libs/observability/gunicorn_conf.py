"""gunicorn hooks for Prometheus multiprocess mode (plan 03-24, CR-02, D-14).

Every API image runs two gunicorn workers. `prometheus_client` keeps one registry per
process, so without multiprocess mode each `/metrics` scrape is answered by whichever
worker accepts the connection: the counters seen by Prometheus jump between two
independent series and `rate()` / `resets()` read thousands of resets (the live evidence
behind CR-02). In multiprocess mode each worker writes its metric values to
memory-mapped files in `PROMETHEUS_MULTIPROC_DIR` and the exposition sums the files, so
every scrape is the total over both workers.

This module only holds the two hooks. It must NOT set the variable itself:
`prometheus_client` chooses its value class (mmap file or plain float) when it is first
imported, which for a gunicorn config module is before any hook runs. The API image's
default command exports the variable in the shell before `exec gunicorn`, so processes
that reuse the same image with a command override (Temporal workers, consumers) never
see the variable and keep their single-process registry and their own metrics port.

- `on_starting` runs once in the gunicorn master before any worker is forked: it creates
  the directory if missing and deletes every file in it, so a restarted container never
  starts from the previous run's counters (T-3-77).
- `child_exit` runs in the master when a worker exits: `mark_process_dead` removes that
  pid's live-gauge files. Counter and histogram files are kept on purpose, so the totals
  of a recycled worker are not lost.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from prometheus_client import multiprocess

MULTIPROC_ENV = "PROMETHEUS_MULTIPROC_DIR"

__all__ = ["MULTIPROC_ENV", "child_exit", "on_starting"]


def on_starting(server: Any) -> None:
    """Create the multiprocess directory and empty it (master, before workers fork)."""
    directory = os.environ.get(MULTIPROC_ENV)
    if not directory:
        return
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    for entry in path.iterdir():
        if entry.is_file() or entry.is_symlink():
            entry.unlink()


def child_exit(server: Any, worker: Any) -> None:
    """Tell prometheus_client that a worker is gone (its live-gauge files are removed)."""
    if not os.environ.get(MULTIPROC_ENV):
        return
    multiprocess.mark_process_dead(worker.pid)
