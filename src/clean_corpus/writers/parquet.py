from __future__ import annotations
import os
from typing import Iterable, Optional
from .base import CorpusWriter
from ..pipeline.context import Document
from ..storage.writer import write_docs_shard

class ParquetCorpusWriter(CorpusWriter):
    name = "parquet"

    def write_shard(
        self,
        docs: Iterable[Document],
        *,
        out_dir: str,
        source: str,
        shard_idx: int,
        document_subpath: Optional[str] = None,
    ) -> str:
        if document_subpath:
            base = os.path.join(out_dir, "documents", document_subpath)
        else:
            base = os.path.join(out_dir, "docs", f"source={source}")
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, f"shard_{shard_idx:06d}.parquet")
        write_docs_shard(path, list(docs))
        return path
