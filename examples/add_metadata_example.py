"""Example: Adding new metadata fields with schema versioning.

This demonstrates how to add new metadata fields without
impacting existing runs or schemas.
"""

from clean_corpus.writers.base import MetadataWriter
from clean_corpus.writers.registry import get_metadata_writer, register_metadata_writer
from clean_corpus.pipeline.context import Document
from typing import Iterable, Dict, Any
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Example: New metadata writer with additional fields
class ParquetMetadataWriterV2(MetadataWriter):
    """Example metadata writer with new fields."""
    
    name = "parquet_v2"
    schema_version = "meta_v2"
    
    def schema(self) -> Dict[str, Any]:
        """Return schema definition."""
        return {
            "schema_version": self.schema_version,
            "columns": [
                # All fields from V1
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
                
                # NEW fields
                ("domain", pa.string()),           # NEW: Domain extracted from URL
                ("content_type", pa.string()),     # NEW: Content type (article, forum, etc.)
                ("word_count", pa.int32()),        # NEW: Word count
            ]
        }
    
    def _schema_arrow(self) -> pa.Schema:
        """Return PyArrow schema."""
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
            
            # NEW fields
            ("domain", pa.string()),
            ("content_type", pa.string()),
            ("word_count", pa.int32()),
        ])
    
    def write_shard(self, docs: Iterable[Document], *, out_dir: str, source: str, shard_idx: int) -> str:
        """Write metadata shard with new schema."""
        path = os.path.join(out_dir, "metadata", f"schema={self.schema_version}",
                           f"source={source}", f"shard_{shard_idx:06d}.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        schema = self._schema_arrow()
        rows = []
        
        for d in docs:
            # Extract domain from URL
            domain = None
            if d.url:
                try:
                    from urllib.parse import urlparse
                    domain = urlparse(d.url).netloc
                except:
                    pass
            
            # Extract content type (example logic)
            content_type = "unknown"
            if d.url:
                if "forum" in d.url.lower() or "reddit" in d.url.lower():
                    content_type = "forum"
                elif "article" in d.url.lower() or "blog" in d.url.lower():
                    content_type = "article"
            
            # Calculate word count
            word_count = len(d.text.split()) if d.text else 0
            
            rows.append({
                # Existing fields
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
                
                # NEW fields
                "domain": domain,
                "content_type": content_type,
                "word_count": word_count,
            })
        
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, path, compression="zstd")
        return path

# Register the new metadata writer
# Note: In production, you'd do this in writers/registry.py
# This is just an example of dynamic registration

def register_metadata_writer(name: str, writer: MetadataWriter):
    """Register a metadata writer dynamically."""
    # This would need to be added to writers/registry.py
    # For now, add to registry.py manually
    pass

# Usage in config:
# output:
#   metadata_format: parquet_v2  # Uses new schema
#
# Old runs with parquet_v1 continue to work independently
