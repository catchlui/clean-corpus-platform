"""Checkpoint store.

Purpose:
- allow long-running builds to resume after crashes/restarts
- preserve progress counters per source
- preserve shard indices

This is a *best-effort* approach for streaming sources.
For strict reproducibility, use snapshot sources and deterministic sharding.

Checkpoint schema (JSON):
{
  "run_id": "...",
  "updated_at_ms": 123,
  "sources": {
    "common_pile": {"processed_docs": 100000, "shard_idx": 12}
  }
}
"""

from __future__ import annotations
from typing import Dict, Any
import os, json, time

class CheckpointStore:
    def __init__(self, out_dir: str, run_id: str):
        self.path = os.path.join(out_dir, "checkpoints", f"{run_id}.json")
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def load(self) -> Dict[str, Any]:
        if not os.path.exists(self.path):
            return {"sources": {}}
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def save(self, state: Dict[str, Any]) -> None:
        state["updated_at_ms"] = int(time.time() * 1000)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self.path)
