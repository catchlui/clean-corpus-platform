"""Analytics event schemas.

Analytics are emitted at *every stage* as small, batch-level events.
Events are stored to Parquet and can also be exported to a metrics system.

This module defines helper constructors and recommended keys, but does not
force strict validation to keep overhead low in large-scale pipelines.
"""

from __future__ import annotations
from typing import Dict, Any
import time

def make_event(
    *,
    run_id: str,
    stage: str,
    source: str,
    layer: str,
    counts: Dict[str, int],
    metrics: Dict[str, float] | None = None,
    rejection_breakdown: Dict[str, int] | None = None,
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "stage": stage,
        "source": source,
        "layer": layer,
        "timestamp_ms": int(time.time() * 1000),
        "counts": counts,
        "metrics": metrics or {},
        "rejection_breakdown": rejection_breakdown or {},
    }
