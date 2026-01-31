"""Core pipeline data model.

Document is the *normalized* representation flowing through stages.
Stages enrich Document with analytics fields and set transform_chain entries.

Design goal:
- Keep Document stable so downstream teams can build on it without churn.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import time

@dataclass
class Document:
    # identity
    doc_id: bytes
    source: str
    text: str

    # provenance
    url: Optional[str] = None
    license: Optional[str] = None
    license_version: Optional[str] = None
    lang: str = "en"
    source_file: Optional[str] = None  # Original file path for multi-file sources

    # cheap stats
    chars: Optional[int] = None
    bytes_utf8: Optional[int] = None
    entropy: Optional[float] = None

    # dedup
    dup_group_id: Optional[int] = None

    # pii
    pii_flag: bool = False
    pii_types: List[str] = field(default_factory=list)

    # extensible (tokenizer/curriculum teams can add)
    tokens: Optional[int] = None
    ppl: Optional[float] = None
    quality_score: Optional[float] = None

    # governance
    policy_version: str = "policy_v0"
    transform_chain: List[str] = field(default_factory=list)
    created_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    # data use tag (training | sft | alignment | etc.) for downstream filtering
    data_tag: Optional[str] = None

    # custom metadata (folder-level metadata, PDF metadata, etc.)
    # Can include: book_name, author, certificate_type, and other custom fields
    extra: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Decision:
    accepted: bool
    stage: str
    reason_code: str = ""
    reason_detail: str = ""
