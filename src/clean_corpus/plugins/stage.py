"""Stage plugin interface (batch-oriented).

We keep per-doc stage interface for local runner, but for Ray Data we also support
batch stages that operate on Arrow Tables / Pandas DataFrames.

Batch stages must:
- accept an Arrow Table or pandas DataFrame
- return (accepted_batch, rejected_rows, analytics_event_fragment)

This keeps performance acceptable for large corpora.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple, List
import pyarrow as pa

class BatchStage(ABC):
    name: str = "batch_stage"
    layer: str = "preprocessing"

    @abstractmethod
    def run_batch(self, batch: pa.Table, *, run_id: str, source: str) -> Tuple[pa.Table, List[Dict[str, Any]], Dict[str, Any]]:
        raise NotImplementedError
