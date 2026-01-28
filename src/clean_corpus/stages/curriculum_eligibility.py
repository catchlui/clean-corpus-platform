"""Curriculum eligibility stage.

Purpose:
- Tag documents as eligible for certain context windows (4K/16K/64K/256K)
- Provide simple signals for curriculum scheduling without entangling training code

This stage is deliberately light:
- uses token count if available (from TokenizeStage)
- uses repetition heuristic (character n-gram repetition proxy) for long-context safety

It does not reject by default; it annotates via transform_chain entries.
Curriculum team can build views using these tags.

Policy YAML example:
```yaml
windows: [4096, 16384, 65536, 262144]
min_tokens_ratio: 0.9         # require doc.tokens >= window * ratio
max_repetition: 0.35          # above this, mark as risky for long windows
```
"""

from __future__ import annotations
from typing import Any, Dict, List
from collections import Counter
from ..pipeline.context import Document, Decision
from .base import Stage

def _repeat_ratio(text: str, n: int = 10, sample_chars: int = 5000) -> float:
    # Cheap repetition proxy: fraction of repeated n-grams in a prefix
    t = text[:sample_chars]
    if len(t) < n * 2:
        return 0.0
    grams = [t[i:i+n] for i in range(0, len(t)-n+1, n)]
    c = Counter(grams)
    total = sum(c.values())
    if total == 0:
        return 0.0
    repeated = sum(v for v in c.values() if v > 1)
    return repeated / total

class CurriculumEligibility(Stage):
    name = "curriculum_eligibility"
    layer = "curriculum"

    def __init__(self, policy: Dict[str, Any]):
        self.windows = [int(x) for x in policy.get("windows", [4096, 16384, 65536, 262144])]
        self.min_ratio = float(policy.get("min_tokens_ratio", 0.9))
        self.max_rep = float(policy.get("max_repetition", 0.35))

    def apply(self, doc: Document) -> Decision:
        # Determine eligibility using token count if available
        eligible = []
        risky = []
        rep = _repeat_ratio(doc.text)
        for w in self.windows:
            if doc.tokens is None:
                # can't compute precisely; skip tagging
                continue
            if doc.tokens >= int(w * self.min_ratio):
                eligible.append(w)
                if w >= 65536 and rep > self.max_rep:
                    risky.append(w)

        if eligible:
            doc.transform_chain.append("eligible_windows=" + ",".join(map(str, eligible)))
        if risky:
            doc.transform_chain.append("risky_windows=" + ",".join(map(str, risky)))
        doc.transform_chain.append(f"rep_ratio={rep:.3f}")
        return Decision(True, self.name)
