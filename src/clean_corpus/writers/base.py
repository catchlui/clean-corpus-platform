"""Output writers.

We separate outputs into:
- CorpusWriter: writes training text (plus minimal required metadata for training)
- MetadataWriter: writes metadata-only tables for curriculum/governance (no text required)

Analytics always remain Parquet via AnalyticsSink.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable, Optional, Dict, Any
from ..pipeline.context import Document

class CorpusWriter(ABC):
    """Writes training corpus shards in a chosen format."""
    name: str

    @abstractmethod
    def write_shard(self, docs: Iterable[Document], *, out_dir: str, source: str, shard_idx: int) -> str:
        """Write a shard and return the output path."""
        raise NotImplementedError

class MetadataWriter(ABC):
    """Writes metadata-only shards (no raw text)."""
    name: str
    schema_version: str

    @abstractmethod
    def write_shard(self, docs: Iterable[Document], *, out_dir: str, source: str, shard_idx: int) -> str:
        raise NotImplementedError

    @abstractmethod
    def schema(self) -> Dict[str, Any]:
        """Return schema definition for documentation/validation."""
        raise NotImplementedError
