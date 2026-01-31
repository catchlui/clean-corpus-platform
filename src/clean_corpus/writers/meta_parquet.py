from __future__ import annotations
import os
from typing import Iterable, Dict, Any, List
import pyarrow as pa
import pyarrow.parquet as pq
from .base import MetadataWriter
from ..pipeline.context import Document

class ParquetMetadataWriterV1(MetadataWriter):
    name = "parquet_v1"
    schema_version = "meta_v1"

    def schema(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "columns": [
                ("doc_id", "binary(32)"),
                ("source", "string"),
                ("lang", "string"),
                ("url", "string"),
                ("license", "string"),
                ("license_version", "string"),
                ("source_file", "string"),  # Original file path for multi-file sources
                ("tokens", "int64"),  # Changed from int32 to int64 to handle large token counts
                ("chars", "int64"),   # Changed from int32 to int64 to handle large documents
                ("bytes_utf8", "int64"),  # Changed from int32 to int64 to handle large documents
                ("entropy", "float32"),
                ("ppl", "float32"),
                ("quality_score", "float32"),
                ("dup_group_id", "int64"),
                ("pii_flag", "bool"),
                ("pii_types", "list<string>"),
                ("policy_version", "string"),
                ("transform_chain", "list<string>"),
                ("created_at_ms", "int64"),
                ("schema_version", "string"),
            ]
        }

    def _schema_arrow(self) -> pa.Schema:
        return pa.schema([
            ("doc_id", pa.binary(32)),
            ("source", pa.string()),
            ("lang", pa.string()),
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
            ("schema_version", pa.string()),
        ])

    def write_shard(self, docs: Iterable[Document], *, out_dir: str, source: str, shard_idx: int) -> str:
        path = os.path.join(out_dir, "metadata", f"schema={self.schema_version}", f"source={source}", f"shard_{shard_idx:06d}.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)

        schema = self._schema_arrow()
        
        # Helper function to safely convert to int64
        def safe_int64(val):
            if val is None:
                return None
            try:
                val_int = int(val)
                max_int64 = 9223372036854775807
                min_int64 = -9223372036854775808
                if val_int > max_int64:
                    return max_int64
                if val_int < min_int64:
                    return min_int64
                return val_int
            except (ValueError, TypeError, OverflowError):
                return None
        
        rows = []
        for d in docs:
            rows.append({
                "doc_id": bytes(d.doc_id) if isinstance(d.doc_id, (bytes, bytearray)) else d.doc_id,
                "source": str(d.source) if d.source else "",
                "lang": str(d.lang) if d.lang else "en",
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
                "schema_version": str(self.schema_version),
            })
        
        # Create table with error handling
        try:
            table = pa.Table.from_pylist(rows, schema=schema)
        except OverflowError as e:
            import logging
            logger = logging.getLogger("clean_corpus.writers.meta_parquet")
            logger.error(f"OverflowError creating metadata table: {e}")
            # Check each row for problematic values
            for idx, row in enumerate(rows):
                for key, value in row.items():
                    if isinstance(value, int):
                        if value > 9223372036854775807 or value < -9223372036854775808:
                            logger.error(f"Row {idx}, field {key}: value {value} exceeds int64 range")
            raise
        
        pq.write_table(table, path, compression="zstd")
        return path
