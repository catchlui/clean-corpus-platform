from __future__ import annotations
import pyarrow as pa
from ..plugins.ray_stage import RayBatchStage
from ..utils.text import char_entropy

class RayQualityGate(RayBatchStage):
    name = "quality_gate"
    layer = "quality"

    def __init__(self, policy):
        self.min_chars = int(policy.get("min_chars", 0))
        ent = policy.get("entropy", {})
        self.ent_min = float(ent.get("min", -1e9))
        self.ent_max = float(ent.get("max", 1e9))

    def run(self, batch: pa.Table, *, run_id: str, source: str, analytics):
        rows = batch.to_pylist()
        accepted = []
        ent_samples = []
        rejected = 0

        for r in rows:
            txt = r.get("text","")
            if len(txt) < self.min_chars:
                rejected += 1
                continue
            ent = char_entropy(txt)
            if not (self.ent_min <= ent <= self.ent_max):
                rejected += 1
                continue
            r["entropy"] = ent
            ent_samples.append(ent)
            accepted.append(r)

        analytics.emit_stage(
            run_id=run_id,
            stage=self.name,
            source=source,
            layer=self.layer,
            input_docs=len(rows),
            accepted_docs=len(accepted),
            rejected_docs=rejected,
            metric_samples={"entropy": ent_samples},
        )
        return pa.Table.from_pylist(accepted)
