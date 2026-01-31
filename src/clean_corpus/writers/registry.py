"""Writer registry.

Add new writers without changing pipeline code by registering them here.

Metadata writers use schema versioning - old schemas continue to work,
new schemas can be added without impacting existing runs.
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

def register_corpus_writer(name: str, writer: CorpusWriter) -> None:
    """Register a new corpus writer dynamically.
    
    Allows adding writers at runtime without modifying this file.
    """
    if name in _CORPUS:
        raise ValueError(f"Corpus writer '{name}' already registered")
    _CORPUS[name] = writer

def register_metadata_writer(name: str, writer: MetadataWriter) -> None:
    """Register a new metadata writer dynamically.
    
    Metadata writers use schema versioning - each version is independent.
    Old runs keep old schemas, new runs can use new schemas.
    """
    if name in _META:
        raise ValueError(f"Metadata writer '{name}' already registered")
    _META[name] = writer

def list_corpus_writers() -> list[str]:
    """List all registered corpus writers."""
    return list(_CORPUS.keys())

def list_metadata_writers() -> list[str]:
    """List all registered metadata writers."""
    return list(_META.keys())

def get_corpus_writer(name: str) -> CorpusWriter:
    """Get corpus writer by name."""
    if name not in _CORPUS:
        raise KeyError(
            f"Unknown corpus writer: {name}. "
            f"Available: {list(_CORPUS)}. "
            f"Register with register_corpus_writer()"
        )
    return _CORPUS[name]

def get_metadata_writer(name: str) -> MetadataWriter:
    """Get metadata writer by name.
    
    Metadata writers are schema-versioned - each version is independent.
    Old runs continue using old schemas, new runs can use new schemas.
    """
    if name not in _META:
        raise KeyError(
            f"Unknown metadata writer: {name}. "
            f"Available: {list(_META)}. "
            f"Register with register_metadata_writer() or create new schema version"
        )
    return _META[name]
