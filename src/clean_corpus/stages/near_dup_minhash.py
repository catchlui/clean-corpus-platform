"""Near-duplicate detection using MinHash (datasketch).

This stage is designed as a **soft gate** by default:
- It assigns `near_dup_cluster` (not yet stored in Document in v0.3 minimal schema)
- It can optionally reject docs if similarity threshold exceeded

For huge scale, you typically:
- cluster and downsample rather than hard-drop everything
- store cluster IDs in a side index

Implementation notes:
- MinHashLSH in-memory is OK for small/medium runs.
- For large corpora, use per-shard LSH + merge, or a service-backed index.
"""

from __future__ import annotations
from typing import Any, Dict
from datasketch import MinHash, MinHashLSH
from ..pipeline.context import Document, Decision
from .base import Stage

def _minhash(text: str, ngram: int, num_perm: int) -> MinHash:
    mh = MinHash(num_perm=num_perm)
    t = text
    if len(t) < ngram:
        mh.update(t.encode("utf-8", errors="ignore"))
        return mh
    for i in range(0, len(t) - ngram + 1, max(1, ngram)):
        mh.update(t[i:i+ngram].encode("utf-8", errors="ignore"))
    return mh

class NearDupMinHash(Stage):
    name = "near_dup_minhash"
    layer = "dedup"

    def __init__(self, threshold: float = 0.9, ngram: int = 5, num_perm: int = 128, hard_reject: bool = False):
        self.threshold = float(threshold)
        self.ngram = int(ngram)
        self.num_perm = int(num_perm)
        self.hard_reject = bool(hard_reject)
        self.lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_perm)
        self._idx = 0

    def apply(self, doc: Document) -> Decision:
        mh = _minhash(doc.text, self.ngram, self.num_perm)
        hits = self.lsh.query(mh)
        if hits:
            # near-duplicate
            doc.transform_chain.append("near_dup_hit_v1")
            if self.hard_reject:
                return Decision(False, self.name, "DUP_NEAR", f"hits={len(hits)}")
        # insert
        key = f"k{self._idx}"
        self._idx += 1
        self.lsh.insert(key, mh)
        doc.transform_chain.append("near_dup_indexed_v1")
        return Decision(True, self.name)
