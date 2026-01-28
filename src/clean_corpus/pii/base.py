"""PII detection primitives.

We separate:
- Detection: find PII signals (type, span, confidence)
- Policy decision: drop / redact / allow / log-only

This design keeps detectors composable and makes policies auditable and expandable.
"""

from __future__ import annotations
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import List, Tuple

@dataclass(frozen=True)
class PIISignal:
    kind: str                 # e.g., "email", "phone", "aadhaar"
    span: Tuple[int, int]     # (start, end) offsets in text
    confidence: float         # 0..1

class PIIDetector(ABC):
    name: str

    @abstractmethod
    def detect(self, text: str) -> List[PIISignal]:
        raise NotImplementedError
