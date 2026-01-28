from __future__ import annotations
import re
from ..base import PIIDetector, PIISignal

# Simple phone heuristic (international/India-ish). Tune per region.
PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d{1,3}[\s-]?)?(?:\d[\s-]?){9,12}(?!\d)")

class PhoneDetector(PIIDetector):
    name = "phone"

    def detect(self, text: str):
        return [PIISignal(kind="phone", span=m.span(), confidence=0.85) for m in PHONE_RE.finditer(text)]
