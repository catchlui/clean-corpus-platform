"""PII policy gate (drop or redact).

Policy YAML example:
```yaml
enabled: true
mode: redact           # drop | redact | allow
drop_kinds: ["aadhaar","email"]
redact_kinds: ["phone","email"]
confidence_threshold: 0.9
```

- If mode=drop: any signal above threshold in drop_kinds triggers rejection.
- If mode=redact: redact spans for redact_kinds; reject for drop_kinds (if any).
- If mode=allow: only annotate doc.pii_flag/types for analytics.

This stage relies on registered detectors in `clean_corpus.pii.registry`.
"""

from __future__ import annotations
from typing import Any, Dict
from ..pipeline.context import Document, Decision
from .base import Stage
# Import pii module to trigger auto-registration
import clean_corpus.pii  # noqa: F401
from ..pii.registry import detect_all
from ..pii.redact import redact_text

class PIIPolicyGate(Stage):
    name = "pii_policy_gate"
    layer = "governance"

    def __init__(self, policy: Dict[str, Any]):
        self.enabled = bool(policy.get("enabled", True))
        self.mode = str(policy.get("mode", "drop")).lower()  # drop|redact|allow
        self.drop_kinds = set(policy.get("drop_kinds", []))
        self.redact_kinds = set(policy.get("redact_kinds", []))
        self.conf_thr = float(policy.get("confidence_threshold", 0.9))

    def apply(self, doc: Document) -> Decision:
        if not self.enabled:
            return Decision(True, self.name)

        signals = [s for s in detect_all(doc.text) if s.confidence >= self.conf_thr]
        if not signals:
            doc.transform_chain.append("pii_none_v1")
            return Decision(True, self.name)

        doc.pii_flag = True
        doc.pii_types = sorted(set(s.kind for s in signals))

        # drop?
        if self.mode in ("drop", "redact"):
            for s in signals:
                if s.kind in self.drop_kinds:
                    return Decision(False, self.name, f"PII_{s.kind.upper()}", "policy_drop")

        # redact?
        if self.mode == "redact":
            to_redact = [s for s in signals if (not self.redact_kinds) or (s.kind in self.redact_kinds)]
            doc.text = redact_text(doc.text, to_redact)
            doc.transform_chain.append("pii_redacted_v1")
            return Decision(True, self.name)

        # allow
        doc.transform_chain.append("pii_allowed_v1")
        return Decision(True, self.name)
