"""Analytics sinks (enhanced).

We emit analytics at every stage.

Two storage layers:
1) Raw events (append-only Parquet): `analytics/events/stage=.../date=.../events.parquet`
2) Aggregates (append-only Parquet): `analytics/aggregates/daily_aggregates.parquet`

Enhancements in v0.3:
- supports percentile summaries (p50/p90/p99) for selected numeric metrics
- stage runners can pass metric samples via event["metric_samples"] = { "entropy": [..], "tokens":[..] }

Dashboards:
- use aggregates for most panels
- raw events for debugging and fine-grained analysis
"""

from __future__ import annotations
from typing import Dict, Any, List
import os
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def _percentiles(xs: List[float], ps=(50, 90, 99)) -> Dict[str, float]:
    if not xs:
        return {}
    arr = np.array(xs, dtype=np.float64)
    out = {}
    for p in ps:
        out[f"p{p}"] = float(np.percentile(arr, p))
    return out

class AnalyticsSink:
    def __init__(self, out_dir: str, run_id: str):
        self.out_dir = out_dir
        self.run_id = run_id
        self.events_dir = os.path.join(out_dir, "analytics", "events")
        self.aggs_dir = os.path.join(out_dir, "analytics", "aggregates")
        os.makedirs(self.events_dir, exist_ok=True)
        os.makedirs(self.aggs_dir, exist_ok=True)

        # in-memory aggregator for a single run; flushed periodically
        self._agg = {}  # key=(date, stage, source) -> counters + percentile accumulators

    def emit(self, event: Dict[str, Any]) -> None:
        stage = event["stage"]
        date = datetime.utcfromtimestamp(event["timestamp_ms"]/1000).date().isoformat()

        # 1) compute percentiles if samples provided
        metric_samples = event.pop("metric_samples", None) or {}
        pct_metrics = {}
        for k, xs in metric_samples.items():
            pct = _percentiles(xs)
            for pk, pv in pct.items():
                pct_metrics[f"{k}_{pk}"] = pv
        # merge pct metrics into metrics dict
        event.setdefault("metrics", {})
        event["metrics"].update(pct_metrics)

        # 2) write raw events
        p = os.path.join(self.events_dir, f"stage={stage}", f"date={date}", "events.parquet")
        os.makedirs(os.path.dirname(p), exist_ok=True)
        self._append_parquet(p, [event])

        # 3) update aggregates
        key = (date, stage, event["source"])
        cur = self._agg.get(key, {
            "date": date, "stage": stage, "source": event["source"],
            "input_docs": 0, "accepted_docs": 0, "rejected_docs": 0,
            # store latest pct metrics; for daily aggregates, last write wins (simple)
        })
        counts = event.get("counts", {})
        cur["input_docs"] += int(counts.get("input_docs", 0))
        cur["accepted_docs"] += int(counts.get("accepted_docs", 0))
        cur["rejected_docs"] += int(counts.get("rejected_docs", 0))

        # copy numeric metrics fields that are already percentiles or aggregates
        for mk, mv in (event.get("metrics", {}) or {}).items():
            if isinstance(mv, (int, float)):
                cur[mk] = float(mv)

        self._agg[key] = cur

    def flush_aggregates(self) -> None:
        if not self._agg:
            return
        rows = list(self._agg.values())
        p = os.path.join(self.aggs_dir, "daily_aggregates.parquet")
        self._append_parquet(p, rows)
        self._agg.clear()

    def _append_parquet(self, path: str, rows: List[Dict[str, Any]]) -> None:
        table = pa.Table.from_pylist(rows)
        if os.path.exists(path):
            existing = pq.read_table(path)
            table = pa.concat_tables([existing, table])
        pq.write_table(table, path, compression="zstd")
