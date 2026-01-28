from __future__ import annotations
import json
from .base import CorpusWriter

class JSONLCorpusWriter(CorpusWriter):
    name = "jsonl"
    def write(self, docs, *, out_dir, source, shard_idx):
        path = f"{out_dir}/docs/source={source}/shard_{shard_idx:06d}.jsonl"
        with open(path, "w", encoding="utf-8") as f:
            for d in docs:
                f.write(json.dumps({
                    "doc_id": d.doc_id.hex(),
                    "text": d.text,
                    "source": d.source,
                    "license": d.license,
                    "transform_chain": d.transform_chain,
                }, ensure_ascii=False) + "\n")
