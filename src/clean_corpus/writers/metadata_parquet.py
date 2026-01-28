from __future__ import annotations
import pyarrow as pa, pyarrow.parquet as pq
from .base import MetadataWriter

class MetadataParquetWriter(MetadataWriter):
    schema_version = "v1"

    def write(self, docs, *, out_dir, source, shard_idx):
        rows = []
        for d in docs:
            rows.append({
                "doc_id": d.doc_id,
                "source": d.source,
                "tokens": d.tokens,
                "entropy": d.entropy,
                "pii_flag": d.pii_flag,
                "pii_types": d.pii_types,
                "schema_version": self.schema_version,
            })
        table = pa.Table.from_pylist(rows)
        path = f"{out_dir}/metadata/source={source}/meta_{shard_idx:06d}.parquet"
        pq.write_table(table, path, compression="zstd")
