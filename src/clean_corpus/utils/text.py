"""Text normalization and cheap quality heuristics."""

from __future__ import annotations
import re
import math
import unicodedata
from collections import Counter

_TAG_RE = re.compile(r"<[^>]+>")

def normalize_unicode_nfc(text: str) -> str:
    """Apply Unicode NFC (Canonical Composition) normalization.
    
    This ensures Indic scripts and other Unicode text have consistent encoding.
    Fixes broken encodings by normalizing to canonical composed form.
    
    Args:
        text: Input text that may have inconsistent Unicode encoding
        
    Returns:
        Text normalized to NFC form
    """
    if not text:
        return text
    try:
        return unicodedata.normalize('NFC', text)
    except (UnicodeError, TypeError):
        # If normalization fails, return original text
        return text

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
