"""Data source plugin interface.

Goal: allow multiple teams to add sources without changing pipeline code.

A source can be:
- streaming (HF streaming, APIs, Kafka)
- batch (local dumps, S3 files)
- incremental (daily drops, CDC)

All sources expose a unified `stream()` generator yielding RawDocument.
"""

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, Optional

class DataSourceType(str, Enum):
    STREAMING = "streaming"
    BATCH = "batch"
    INCREMENTAL = "incremental"
    SNAPSHOT = "snapshot"

@dataclass
class RawDocument:
    raw_id: str
    text: str
    source: str
    url: Optional[str] = None
    license: Optional[str] = None
    created_at: Optional[str] = None
    extra: Dict[str, Any] = None

@dataclass
class SourceSpec:
    name: str
    type: str               # streaming|batch|incremental|snapshot
    kind: str               # implementation key, e.g., hf_stream
    dataset: str            # for hf_stream
    split: str = "train"
    text_field: str = "text"
    license_field: str = "license"
    url_field: str = "url"

class DataSource:
    """Base interface for all sources."""
    name: str
    source_type: DataSourceType

    def schema(self) -> Dict[str, str]:
        return {"text": "string", "license": "string", "url": "string"}

    def metadata(self) -> Dict[str, Any]:
        return {}

    def stream(self) -> Iterable[RawDocument]:
        raise NotImplementedError


from clean_corpus.utils.fingerprint import stable_fingerprint

def _source_fingerprint(spec_dict: dict) -> str:
    return stable_fingerprint(spec_dict)
