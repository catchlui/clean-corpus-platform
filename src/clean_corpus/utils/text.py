"""Text normalization and cheap quality heuristics."""

from __future__ import annotations
import re
import math
from collections import Counter

_TAG_RE = re.compile(r"<[^>]+>")

def sanitize(text: str) -> str:
    """Remove simple HTML tags and normalize whitespace."""
    text = _TAG_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def char_entropy(text: str) -> float:
    """Character-level Shannon entropy.

Low entropy often indicates boilerplate/templates.
Very high entropy often indicates encoding junk.
"""
    if not text:
        return 0.0
    c = Counter(text)
    n = sum(c.values())
    return -sum((v/n) * math.log2(v/n) for v in c.values())
