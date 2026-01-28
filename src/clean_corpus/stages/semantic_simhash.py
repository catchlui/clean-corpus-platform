"""Semantic hashing using SimHash-style signature.

We intentionally avoid heavy embeddings in this baseline.
Instead, we compute a semantic-ish signature from tokenized word features.

- Fast and dependency-light
- Useful for detecting paraphrase-y near-duplicates and boilerplate variants
- Best used for clustering/analytics; hard rejection should be conservative

If you want embedding-based hashing later, add a stage that calls your embedding service/model.
"""

from __future__ import annotations
from typing import List
import re
from ..pipeline.context import Document, Decision
from .base import Stage

_WORD_RE = re.compile(r"[A-Za-z0-9_]{2,}")

def _simhash64(tokens: List[str]) -> int:
    # Standard SimHash: weighted bit sum over hashed features
    v = [0] * 64
    for t in tokens:
        h = hash(t)
        for i in range(64):
            bit = (h >> i) & 1
            v[i] += 1 if bit else -1
    out = 0
    for i in range(64):
        if v[i] > 0:
            out |= (1 << i)
    return out & ((1<<64)-1)

class SemanticSimHash(Stage):
    name = "semantic_simhash"
    layer = "semantic"

    def __init__(self, max_tokens: int = 2000):
        self.max_tokens = int(max_tokens)

    def apply(self, doc: Document) -> Decision:
        feats = _WORD_RE.findall(doc.text.lower())[: self.max_tokens]
        sig = _simhash64(feats)
        # store signature as hex in transform chain for now; in prod store in side index parquet
        doc.transform_chain.append(f"simhash64={sig:016x}")
        return Decision(True, self.name)
