"""Tokenization stage (plugin-driven).

- Uses a TokenizerAdapter registered in `clean_corpus.plugins.registry`.
- Computes:
  - tokens (count)
  - cheap token stats (unique tokens, top coverage, hapax ratio)
- Emits analytics percentiles via AnalyticsSink (handled centrally).

This stage is optional; if tokenizer is not registered, it will skip and accept.
"""

from __future__ import annotations
from typing import Dict, Any
from collections import Counter
import numpy as np

from ..pipeline.context import Document, Decision
from .base import Stage
from ..plugins.registry import get_tokenizer

class TokenizeStage(Stage):
    name = "tokenize"
    layer = "tokenization"

    def __init__(self, tokenizer_name: str):
        self.tokenizer_name = tokenizer_name

    def apply(self, doc: Document) -> Decision:
        tok = get_tokenizer(self.tokenizer_name)
        if tok is None:
            # No tokenizer registered: skip
            doc.transform_chain.append("tokenize_skipped_no_adapter")
            return Decision(True, self.name)

        ids = tok.encode(doc.text)
        doc.tokens = int(len(ids))

        # Lightweight token stats (avoid storing ids)
        if doc.tokens > 0:
            c = Counter(ids)
            doc_unique = len(c)
            freqs = np.array(sorted(c.values(), reverse=True), dtype=np.int64)
            top10 = freqs[:10].sum() / doc.tokens if freqs.size else 0.0
            hapax = float(sum(1 for v in c.values() if v == 1)) / max(1, doc_unique)
            # stash in quality_score placeholder for now; teams can expand Document schema later
            # here we keep it minimal; use transform_chain to indicate enrichment
            doc.quality_score = float(max(0.0, min(1.0, 1.0 - hapax*0.2)))  # simple heuristic

        doc.transform_chain.append("tokenize_v1")
        return Decision(True, self.name)
