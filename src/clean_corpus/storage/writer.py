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
