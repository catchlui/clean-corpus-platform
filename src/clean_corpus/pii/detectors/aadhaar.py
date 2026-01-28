from __future__ import annotations
import re
from ..base import PIIDetector, PIISignal

# Aadhaar: 12 digits often grouped as 4-4-4
AADHAAR_RE = re.compile(r"(?<!\d)(?:\d{4}[\s-]?){2}\d{4}(?!\d)")

class AadhaarDetector(PIIDetector):
    name = "aadhaar"

    def detect(self, text: str):
        return [PIISignal(kind="aadhaar", span=m.span(), confidence=0.95) for m in AADHAAR_RE.finditer(text)]
