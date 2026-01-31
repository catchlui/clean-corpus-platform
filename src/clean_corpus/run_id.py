"""Run ID resolution: explicit or auto-generated from config.

Auto-generation uses:
- prefix_digits / suffix_digits: first/last N digits of a compact timestamp
- include_input_name: name derived from first source (source name or input directory)
"""

from __future__ import annotations
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _timestamp_digits(prefix: int = 4, suffix: int = 6) -> tuple[str, str]:
    """Compact timestamp YYYYMMDDHHMMSS; return (first prefix_digits, last suffix_digits)."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")  # 14 digits
    a = ts[: min(prefix, len(ts))]
    b = ts[-min(suffix, len(ts)) :] if suffix else ""
    return (a, b)


def _input_name_from_sources(cfg: Dict[str, Any], from_kind: str) -> str:
    """Derive a short name from the first source for use in run_id."""
    sources = cfg.get("sources") or []
    if not sources:
        return "run"
    first = sources[0]
    if from_kind == "source":
        name = first.get("name") or "run"
    elif from_kind == "dataset":
        raw = first.get("dataset")
        if isinstance(raw, list):
            raw = raw[0] if raw else ""
        if not raw:
            return first.get("name") or "run"
        raw = str(raw).strip()
        # Path: use parent dir for files, else dir name
        if "/" in raw or "\\" in raw or os.path.sep in raw:
            normalized = os.path.normpath(raw)
            if os.path.splitext(normalized)[1]:
                name = os.path.basename(os.path.dirname(normalized))
            else:
                name = os.path.basename(normalized)
        else:
            # e.g. "allenai/dolma" -> "dolma"
            name = raw.split("/")[-1].split("\\")[-1]
        if not name:
            name = first.get("name") or "run"
    else:
        name = first.get("name") or "run"
    # Safe for run_id: alphanumeric and underscore
    name = re.sub(r"[^\w\-]", "_", name)
    return name or "run"


def generate_run_id(cfg: Dict[str, Any], auto_cfg: Dict[str, Any]) -> str:
    """Build run_id from run_id_auto config and pipeline config.

    auto_cfg may contain:
    - prefix_digits: first N digits of timestamp (default 4 → year)
    - suffix_digits: last N digits of timestamp (default 6 → time-ish)
    - include_input_name: bool, include name from first source (default True)
    - input_name_from: "source" | "dataset" — use source name or dataset path (default "source")
    - separator: string between parts (default "_")
    """
    prefix_digits = int(auto_cfg.get("prefix_digits", 4))
    suffix_digits = int(auto_cfg.get("suffix_digits", 6))
    include_input_name = auto_cfg.get("include_input_name", True)
    input_name_from = auto_cfg.get("input_name_from", "source")
    separator = str(auto_cfg.get("separator", "_"))

    parts: list[str] = []
    if include_input_name:
        parts.append(_input_name_from_sources(cfg, input_name_from))
    pre, suf = _timestamp_digits(prefix_digits, suffix_digits)
    if pre:
        parts.append(pre)
    if suf:
        parts.append(suf)
    return separator.join(parts) if parts else "run"


def resolve_run_id(cfg: Dict[str, Any]) -> str:
    """Return run_id: explicit run.run_id, or auto-generated from run.run_id_auto, or 'run'."""
    run = cfg.get("run") or {}
    explicit = run.get("run_id")
    if explicit is not None and str(explicit).strip():
        return str(explicit).strip()
    auto_cfg = run.get("run_id_auto")
    if isinstance(auto_cfg, dict) and auto_cfg.get("enabled", True):
        return generate_run_id(cfg, auto_cfg)
    return "run"


def resolve_out_dir(cfg: Dict[str, Any], run_id: str) -> str:
    """Return out_dir with {run_id} placeholder replaced by the resolved run_id."""
    run = cfg.get("run") or {}
    out_dir = run.get("out_dir") or "storage"
    if "{run_id}" in out_dir:
        return out_dir.replace("{run_id}", run_id)
    return out_dir
