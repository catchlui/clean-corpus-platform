"""Policy diff tool.

Compares two YAML policy files and prints a structured diff.

Usage:
`clean-corpus policy-diff --a policies/pii.yaml --b policies/pii_v2.yaml`

We implement a simple recursive diff to avoid extra dependencies.
"""

from __future__ import annotations
from typing import Any, Dict, List, Tuple
import yaml

def _load(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def diff(a: Any, b: Any, prefix: str = "") -> List[Tuple[str, str, Any, Any]]:
    """Return list of (path, change_type, old, new)."""
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        keys = set(a.keys()) | set(b.keys())
        for k in sorted(keys):
            pa = a.get(k, None)
            pb = b.get(k, None)
            pfx = f"{prefix}.{k}" if prefix else str(k)
            if k not in a:
                out.append((pfx, "added", None, pb))
            elif k not in b:
                out.append((pfx, "removed", pa, None))
            else:
                out.extend(diff(pa, pb, pfx))
    elif isinstance(a, list) and isinstance(b, list):
        if a != b:
            out.append((prefix, "changed", a, b))
    else:
        if a != b:
            out.append((prefix, "changed", a, b))
    return out

def render(diff_rows: List[Tuple[str,str,Any,Any]]) -> str:
    lines = []
    for path, typ, old, new in diff_rows:
        if typ == "added":
            lines.append(f"+ {path}: {new}")
        elif typ == "removed":
            lines.append(f"- {path}: {old}")
        else:
            lines.append(f"~ {path}: {old} -> {new}")
    return "\n".join(lines)

def main(a_path: str, b_path: str) -> str:
    a = _load(a_path)
    b = _load(b_path)
    rows = diff(a, b)
    return render(rows)
