"""Stage plugin interface.

Stages must:
- accept a Document
- return a Decision (accept/reject + reason)
- optionally mutate/enrich Document
- emit transform_chain entry on success (for auditability)

Stages should be cheap; heavy work (embeddings, topics, perplexity) should be optional
or run on sampled subsets, depending on budget.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from ..pipeline.context import Document, Decision

class Stage(ABC):
    name: str = "stage"
    layer: str = "preprocessing"

    @abstractmethod
    def apply(self, doc: Document) -> Decision:
        ...
