"""Logging utilities.

We use Python's standard `logging` module with a JSON-ish structured format.
This keeps dependencies minimal and makes logs easy to ship to ELK/Loki.

- Logs go to: `<out_dir>/logs/<run_id>.log`
- Also prints concise progress to stdout.
"""

from __future__ import annotations
import logging
import os
from datetime import datetime

def setup_logging(out_dir: str, run_id: str) -> None:
    os.makedirs(os.path.join(out_dir, "logs"), exist_ok=True)
    log_path = os.path.join(out_dir, "logs", f"{run_id}.log")

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # File
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)
