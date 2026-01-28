"""Redaction utilities.

We perform span-based redaction. Policy can choose:
- drop: reject document
- redact: mask spans with tokens like <PII:EMAIL>
- allow: keep
"""

from __future__ import annotations
from typing import List
from .base import PIISignal

def redact_text(text: str, signals: List[PIISignal]) -> str:
    if not signals:
        return text
    # Sort spans descending so offsets don't shift
    sigs = sorted(signals, key=lambda s: s.span[0], reverse=True)
    out = text
    for s in sigs:
        a, b = s.span
        a = max(0, min(a, len(out)))
        b = max(0, min(b, len(out)))
        if a >= b:
            continue
        out = out[:a] + f"<PII:{s.kind.upper()}>" + out[b:]
    return out
