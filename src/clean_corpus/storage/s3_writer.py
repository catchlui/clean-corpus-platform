"""S3 storage utilities for writing files to S3."""

from __future__ import annotations
import io
import json
from typing import List, Dict, Any
import pyarrow as pa
import pyarrow.parquet as pq

def write_parquet_to_s3(storage_backend, s3_key: str, table: pa.Table, compression: str = "zstd") -> None:
    """Write Parquet table to S3."""
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression=compression)
    buffer.seek(0)
    storage_backend.write_file(s3_key, buffer.read())

def append_jsonl_to_s3(storage_backend, s3_key: str, rows: List[Dict[str, Any]]) -> None:
    """Append JSONL rows to S3 file."""
    # For S3, we need to read existing content, append, and write back
    # This is inefficient for large files - consider using S3 multipart upload
    # or writing to separate files and merging later
    
    existing_content = b""
    if storage_backend.exists(s3_key):
        existing_content = storage_backend.read_file(s3_key)
    
    new_lines = [json.dumps(r, ensure_ascii=False) + "\n" for r in rows]
    new_content = existing_content + "".join(new_lines).encode('utf-8')
    
    storage_backend.write_file(s3_key, new_content)

def write_json_to_s3(storage_backend, s3_key: str, data: Dict[str, Any]) -> None:
    """Write JSON to S3."""
    content = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
    storage_backend.write_file(s3_key, content)
