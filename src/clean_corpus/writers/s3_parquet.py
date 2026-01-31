"""S3 Parquet writer.

Writes Parquet files directly to S3.
"""

from __future__ import annotations
import io
from typing import Iterable
from .base import CorpusWriter
from ..pipeline.context import Document
from ..storage.writer import docs_schema
import pyarrow as pa
import pyarrow.parquet as pq

class S3ParquetCorpusWriter(CorpusWriter):
    """Parquet writer that writes to S3."""
    name = "s3_parquet"
    
    def __init__(self, storage_backend):
        self.storage = storage_backend
    
    def write_shard(self, docs: Iterable[Document], *, out_dir: str, source: str, shard_idx: int) -> str:
        """Write shard to S3."""
        # Build S3 key
        s3_key = self.storage.join(out_dir, "docs", f"source={source}", f"shard_{shard_idx:06d}.parquet")
        
        # Convert docs to Parquet in memory
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
        
        # Write to buffer then upload to S3
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="zstd")
        buffer.seek(0)
        
        self.storage.write_file(s3_key, buffer.read())
        
        return s3_key
