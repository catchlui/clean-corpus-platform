"""Writer registry.

Add new writers without changing pipeline code by registering them here.
"""

from __future__ import annotations
from typing import Dict
from .base import CorpusWriter, MetadataWriter
from .parquet import ParquetCorpusWriter
from .jsonl import JSONLCorpusWriter
from .meta_parquet import ParquetMetadataWriterV1

_CORPUS: Dict[str, CorpusWriter] = {
    "parquet": ParquetCorpusWriter(),
    "jsonl": JSONLCorpusWriter(),
}

_META: Dict[str, MetadataWriter] = {
    "parquet_v1": ParquetMetadataWriterV1(),
}

def get_corpus_writer(name: str) -> CorpusWriter:
    if name not in _CORPUS:
        raise KeyError(f"Unknown corpus writer: {name}. Available: {list(_CORPUS)}")
    return _CORPUS[name]

def get_metadata_writer(name: str) -> MetadataWriter:
    if name not in _META:
        raise KeyError(f"Unknown metadata writer: {name}. Available: {list(_META)}")
    return _META[name]
