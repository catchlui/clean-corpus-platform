"""Parquet writers.

We keep writers simple and robust:
- write shards of `docs.parquet` for training consumption
- write `rejections.jsonl` for auditability (append-only)
- write a run manifest at the end

For very large scale, move to Ray Data write_parquet or Arrow Dataset API.
"""

from __future__ import annotations
from typing import List, Dict, Any
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
from ..pipeline.context import Document

def docs_schema() -> pa.Schema:
    return pa.schema([
        ("doc_id", pa.binary(32)),
        ("source", pa.string()),
        ("lang", pa.string()),
        ("text", pa.string()),
        ("url", pa.string()),
        ("license", pa.string()),
        ("license_version", pa.string()),
        ("tokens", pa.int32()),
        ("chars", pa.int32()),
        ("bytes_utf8", pa.int32()),
        ("entropy", pa.float32()),
        ("ppl", pa.float32()),
        ("quality_score", pa.float32()),
        ("dup_group_id", pa.int64()),
        ("pii_flag", pa.bool_()),
        ("pii_types", pa.list_(pa.string())),
        ("policy_version", pa.string()),
        ("transform_chain", pa.list_(pa.string())),
        ("created_at_ms", pa.int64()),
    ])

def write_docs_shard(path: str, docs: List[Document]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    schema = docs_schema()
    table = pa.Table.from_pylist([{
        "doc_id": d.doc_id,
        "source": d.source,
        "lang": d.lang,
        "text": d.text,
        "url": d.url,
        "license": d.license,
        "license_version": d.license_version,
        "tokens": d.tokens,
        "chars": d.chars,
        "bytes_utf8": d.bytes_utf8,
        "entropy": d.entropy,
        "ppl": d.ppl,
        "quality_score": d.quality_score,
        "dup_group_id": d.dup_group_id,
        "pii_flag": d.pii_flag,
        "pii_types": d.pii_types,
        "policy_version": d.policy_version,
        "transform_chain": d.transform_chain,
        "created_at_ms": d.created_at_ms,
    } for d in docs], schema=schema)
    pq.write_table(table, path, compression="zstd")

def append_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_manifest(path: str, manifest: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
