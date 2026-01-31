"""PII detector registry.

Teams can add new detectors by:
1) implementing PIIDetector in `clean_corpus.pii.detectors.*` (or external package)
2) calling `register_detector(detector)` at startup

Built-in detectors are auto-registered on import via clean_corpus.pii.__init__
"""

from __future__ import annotations
from typing import List
from .base import PIIDetector, PIISignal

_DETECTORS: List[PIIDetector] = []

def register_detector(detector: PIIDetector) -> None:
    """Register a PII detector. Duplicate names are ignored."""
    # Check if detector with same name already exists
    existing_names = [d.name for d in _DETECTORS]
    if detector.name not in existing_names:
        _DETECTORS.append(detector)

def list_detectors() -> List[str]:
    return [d.name for d in _DETECTORS]

def detect_all(text: str) -> List[PIISignal]:
    """Run all registered detectors on text."""
    signals: List[PIISignal] = []
    for d in _DETECTORS:
        signals.extend(d.detect(text))
    return signals
