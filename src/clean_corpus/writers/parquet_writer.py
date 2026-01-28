from __future__ import annotations
from .base import CorpusWriter
from ..storage.writer import write_docs_shard

class ParquetCorpusWriter(CorpusWriter):
    name = "parquet"
    def write(self, docs, *, out_dir, source, shard_idx):
        path = f"{out_dir}/docs/source={source}/shard_{shard_idx:06d}.parquet"
        write_docs_shard(path, list(docs))
