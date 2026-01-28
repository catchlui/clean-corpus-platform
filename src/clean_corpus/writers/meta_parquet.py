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
                ("tokens", "int32"),
                ("chars", "int32"),
                ("bytes_utf8", "int32"),
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
            ("schema_version", pa.string()),
        ])

    def write_shard(self, docs: Iterable[Document], *, out_dir: str, source: str, shard_idx: int) -> str:
        path = os.path.join(out_dir, "metadata", f"schema={self.schema_version}", f"source={source}", f"shard_{shard_idx:06d}.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)

        schema = self._schema_arrow()
        rows = []
        for d in docs:
            rows.append({
                "doc_id": d.doc_id,
                "source": d.source,
                "lang": d.lang,
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
                "schema_version": self.schema_version,
            })
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, path, compression="zstd")
        return path
