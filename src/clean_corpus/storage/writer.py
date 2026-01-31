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
        ("source_file", pa.string()),  # Original file path for multi-file sources
        ("tokens", pa.int64()),  # Changed from int32 to int64 to handle large token counts
        ("chars", pa.int64()),   # Changed from int32 to int64 to handle large documents
        ("bytes_utf8", pa.int64()),  # Changed from int32 to int64 to handle large documents
        ("entropy", pa.float32()),
        ("ppl", pa.float32()),
        ("quality_score", pa.float32()),
        ("dup_group_id", pa.int64()),
        ("pii_flag", pa.bool_()),
        ("pii_types", pa.list_(pa.string())),
        ("policy_version", pa.string()),
        ("transform_chain", pa.list_(pa.string())),
        ("created_at_ms", pa.int64()),
    ], metadata={"schema_version": "v2"})  # Version bump due to int32->int64 change

def write_docs_shard(path: str, docs: List[Document]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    schema = docs_schema()
    # Convert None values and ensure proper types for PyArrow
    rows = []
    for d in docs:
        # Ensure integer values are within int64 range and handle None
        def safe_int64(val):
            if val is None:
                return None
            try:
                # Ensure it's a Python int and within int64 range
                val_int = int(val)
                # int64 range: -2^63 to 2^63-1
                max_int64 = 9223372036854775807
                min_int64 = -9223372036854775808
                if val_int > max_int64:
                    return max_int64  # Clamp to max int64
                if val_int < min_int64:
                    return min_int64  # Clamp to min int64
                return val_int
            except (ValueError, TypeError, OverflowError):
                return None
        
        row = {
            "doc_id": bytes(d.doc_id) if isinstance(d.doc_id, (bytes, bytearray)) else d.doc_id,
            "source": str(d.source) if d.source else "",
            "lang": str(d.lang) if d.lang else "en",
            "text": str(d.text) if d.text else "",
            "url": str(d.url) if d.url else "",
            "license": str(d.license) if d.license else "",
            "license_version": str(d.license_version) if d.license_version else "",
            "source_file": str(d.source_file) if d.source_file else "",
            "tokens": safe_int64(d.tokens),
            "chars": safe_int64(d.chars),
            "bytes_utf8": safe_int64(d.bytes_utf8),
            "entropy": float(d.entropy) if d.entropy is not None else None,
            "ppl": float(d.ppl) if d.ppl is not None else None,
            "quality_score": float(d.quality_score) if d.quality_score is not None else None,
            "dup_group_id": safe_int64(d.dup_group_id),
            "pii_flag": bool(d.pii_flag) if d.pii_flag is not None else False,
            "pii_types": [str(x) for x in (d.pii_types or [])],
            "policy_version": str(d.policy_version) if d.policy_version else "policy_v0",
            "transform_chain": [str(x) for x in (d.transform_chain or [])],
            "created_at_ms": safe_int64(d.created_at_ms) if d.created_at_ms is not None else 0,
        }
        rows.append(row)
    
    # Create table with explicit schema
    # If there's an overflow error, it's likely due to a value exceeding int64 range
    # or a type conversion issue
    try:
        table = pa.Table.from_pylist(rows, schema=schema)
    except OverflowError as e:
        # Debug: check which values might be problematic
        import logging
        logger = logging.getLogger("clean_corpus.storage.writer")
        logger.error(f"OverflowError creating table: {e}")
        # Check each row for problematic values
        for idx, row in enumerate(rows):
            for key, value in row.items():
                if isinstance(value, int):
                    if value > 9223372036854775807 or value < -9223372036854775808:
                        logger.error(f"Row {idx}, field {key}: value {value} exceeds int64 range")
        raise
    except Exception as e:
        import logging
        logger = logging.getLogger("clean_corpus.storage.writer")
        logger.error(f"Error creating table: {e}")
        raise
    
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
