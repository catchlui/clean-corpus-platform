"""Logging utilities.

We use Python's standard `logging` module with a JSON-ish structured format.
This keeps dependencies minimal and makes logs easy to ship to ELK/Loki.

- Logs go to: `<log_dir>/<run_id>.log` (global log directory)
- Also prints concise progress to stdout.
"""

from __future__ import annotations
import logging
import os
from datetime import datetime
from typing import Optional

def setup_logging(out_dir: str, run_id: str, log_dir: Optional[str] = None) -> None:
    """
    Setup logging configuration.
    
    Args:
        out_dir: Output directory (for backward compatibility)
        run_id: Run identifier
        log_dir: Global log directory (if None, uses out_dir/logs for backward compatibility)
    """
    if log_dir is None:
        # Backward compatibility: use out_dir/logs
        log_dir = os.path.join(out_dir, "logs")
    
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{run_id}.log")

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
