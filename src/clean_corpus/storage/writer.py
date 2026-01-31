from __future__ import annotations
import json
import os
from typing import Any, Dict, Iterable, List

import pyarrow as pa
import pyarrow.parquet as pq

from ..pipeline.context import Document


def docs_schema() -> pa.Schema:
    """Return the shared schema for Parquet documents."""
    return pa.schema(
        [
            ("doc_id", pa.binary(32)),
            ("source", pa.string()),
            ("lang", pa.string()),
            ("text", pa.string()),
            ("url", pa.string()),
            ("license", pa.string()),
            ("license_version", pa.string()),
            ("source_file", pa.string()),
            ("tokens", pa.int64()),
            ("chars", pa.int64()),
            ("bytes_utf8", pa.int64()),
            ("entropy", pa.float32()),
            ("ppl", pa.float32()),
            ("quality_score", pa.float32()),
            ("dup_group_id", pa.int64()),
            ("pii_flag", pa.bool_()),
            ("pii_types", pa.list_(pa.string())),
            ("policy_version", pa.string()),
            ("transform_chain", pa.list_(pa.string())),
            ("created_at_ms", pa.int64()),
            ("data_tag", pa.string()),
        ]
    )


def _doc_to_row(doc: Document) -> Dict[str, Any]:
    def safe_int(value: Any) -> Any:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    return {
        "doc_id": bytes(doc.doc_id) if isinstance(doc.doc_id, (bytes, bytearray)) else doc.doc_id,
        "source": doc.source or "",
        "lang": doc.lang or "en",
        "text": doc.text or "",
        "url": doc.url or "",
        "license": doc.license or "",
        "license_version": doc.license_version or "",
        "source_file": doc.source_file or "",
        "tokens": safe_int(doc.tokens),
        "chars": safe_int(doc.chars),
        "bytes_utf8": safe_int(doc.bytes_utf8),
        "entropy": float(doc.entropy) if doc.entropy is not None else None,
        "ppl": float(doc.ppl) if doc.ppl is not None else None,
        "quality_score": float(doc.quality_score) if doc.quality_score is not None else None,
        "dup_group_id": safe_int(doc.dup_group_id),
        "pii_flag": bool(doc.pii_flag),
        "pii_types": [str(x) for x in (doc.pii_types or [])],
        "policy_version": doc.policy_version or "",
        "transform_chain": [str(x) for x in (doc.transform_chain or [])],
        "created_at_ms": safe_int(doc.created_at_ms) or 0,
        "data_tag": doc.data_tag or "",
    }


def write_docs_shard(path: str, docs: Iterable[Document]) -> None:
    """Write documents to Parquet using shared schema."""
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    rows = [_doc_to_row(d) for d in docs]
    if not rows:
        return
    table = pa.Table.from_pylist(rows, schema=docs_schema())
    pq.write_table(table, path, compression="zstd")


def append_jsonl(path: str, items: List[Dict[str, Any]]) -> None:
    """Append JSON lines to the given file."""
    if not items:
        return
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        for item in items:
            fh.write(json.dumps(item, ensure_ascii=False) + "\n")


def write_manifest(path: str, manifest: Dict[str, Any]) -> None:
    """Write manifest JSON (overwrites existing)."""
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)
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
        ("extra_metadata", pa.string()),  # JSON string containing custom metadata (book_name, author, certificate_type, etc.)
    ], metadata={"schema_version": "v3"})  # Version bump: added extra_metadata field

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
        
        # Add extra metadata (folder-level metadata, PDF metadata, etc.)
        # Note: For Parquet, we store extra metadata as a JSON string in a separate column
        # This includes: book_name, author, certificate_type, pdf_metadata, etc.
        if d.extra:
            # Store extra metadata as JSON string for Parquet compatibility
            import json
            row["extra_metadata"] = json.dumps(d.extra, ensure_ascii=False)
        else:
            row["extra_metadata"] = None
        
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
