from __future__ import annotations
import re
from ..base import PIIDetector, PIISignal

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")

class EmailDetector(PIIDetector):
    name = "email"

    def detect(self, text: str):
        return [PIISignal(kind="email", span=m.span(), confidence=0.99) for m in EMAIL_RE.finditer(text)]
