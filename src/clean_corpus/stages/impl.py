"""Built-in stages (minimal set).

These implement:
- license gate
- sanitize
- exact dedup
- quality gate (length + entropy)
- simple PII gate (email regex)

Note: MinHash near-dedup, semantic hashing, topic labeling, and custom tokenization
are intentionally left as additional plugins.
"""

from __future__ import annotations
from typing import Any, Dict, Set
import re
from ..pipeline.context import Document, Decision
from ..utils.text import sanitize, char_entropy
from ..utils.hashing import sha256_bytes
from .base import Stage

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")

class LicenseGate(Stage):
    name = "license_gate"
    layer = "governance"

    def __init__(self, policy: Dict[str, Any]):
        self.allowed = set(policy.get("allowed_licenses", []))
        self.disallowed = set(policy.get("disallowed_licenses", []))

    def apply(self, doc: Document) -> Decision:
        lic = doc.license
        if lic is None:
            return Decision(False, self.name, "LICENSE_UNKNOWN", "missing license metadata")
        if lic in self.disallowed:
            return Decision(False, self.name, "LICENSE_DISALLOWED", f"license={lic}")
        if self.allowed and lic not in self.allowed:
            return Decision(False, self.name, "LICENSE_NOT_ALLOWED", f"license={lic}")
        doc.transform_chain.append("license_gate_v1")
        return Decision(True, self.name)

class Sanitize(Stage):
    name = "sanitize"
    layer = "preprocessing"

    def apply(self, doc: Document) -> Decision:
        doc.text = sanitize(doc.text)
        doc.transform_chain.append("sanitize_v1")
        return Decision(True, self.name)

class ExactDedup(Stage):
    name = "exact_dedup"
    layer = "preprocessing"

    def __init__(self):
        # For distributed runs replace with a shared store (Bloom/Redis/RocksDB).
        self.seen: Set[bytes] = set()

    def apply(self, doc: Document) -> Decision:
        h = sha256_bytes(doc.text)
        if h in self.seen:
            return Decision(False, self.name, "DUP_EXACT", "duplicate content hash")
        self.seen.add(h)

        doc.doc_id = h
        doc.dup_group_id = int.from_bytes(h[:8], "big", signed=False)
        doc.transform_chain.append("exact_dedup_v1")
        return Decision(True, self.name)

class QualityGate(Stage):
    name = "quality_gate"
    layer = "quality"

    def __init__(self, policy: Dict[str, Any]):
        self.min_chars = int(policy.get("min_chars", 0))
        ent = policy.get("entropy", {}) or {}
        self.ent_min = float(ent.get("min", -1e9))
        self.ent_max = float(ent.get("max", +1e9))

    def apply(self, doc: Document) -> Decision:
        doc.chars = len(doc.text)
        doc.bytes_utf8 = len(doc.text.encode("utf-8", errors="ignore"))
        if doc.chars < self.min_chars:
            return Decision(False, self.name, "TOO_SHORT", f"chars={doc.chars}")
        doc.entropy = char_entropy(doc.text)
        if not (self.ent_min <= doc.entropy <= self.ent_max):
            return Decision(False, self.name, "ENTROPY_OUT_OF_RANGE", f"entropy={doc.entropy:.3f}")
        doc.transform_chain.append("quality_gate_v1")
        return Decision(True, self.name)

class PIIGate(Stage):
    name = "pii_gate"
    layer = "governance"

    def __init__(self, policy: Dict[str, Any]):
        self.enabled = bool(policy.get("enabled", True))
        self.drop = bool(policy.get("drop_if_detected", True))

    def apply(self, doc: Document) -> Decision:
        if not self.enabled:
            return Decision(True, self.name)
        if EMAIL_RE.search(doc.text):
            doc.pii_flag = True
            doc.pii_types = ["email"]
            if self.drop:
                return Decision(False, self.name, "PII_EMAIL", "email detected")
        doc.transform_chain.append("pii_gate_v1")
        return Decision(True, self.name)
