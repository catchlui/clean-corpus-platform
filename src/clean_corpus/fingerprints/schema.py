"""Global fingerprint schema and decision types.

Schema supports: Have we seen this before? Where did it come from? Is this a partial duplicate?
Versioning (fingerprint_version, hash_params) allows re-dedupe without re-ingesting.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
import time
import uuid


class FingerprintType(str, Enum):
    SIMHASH = "simhash"
    MINHASH = "minhash"
    CHUNK_HASH = "chunk_hash"


class DedupAction(str, Enum):
    DROP = "drop"
    KEEP = "keep"
    KEEP_LINK = "keep_link"  # Keep but link to original


@dataclass
class HashParams:
    """Versioned hash parameters for fingerprint evolution."""
    fingerprint_version: str = "v1"
    shingle_size: int = 5
    num_hashes: int = 128
    max_tokens: int = 2000  # SimHash
    chunk_size: int = 512   # Chunk hash
    chunk_overlap: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FingerprintRecord:
    """Single global fingerprint entry. No full text—only fingerprint + minimal metadata."""
    fingerprint_id: str
    fingerprint_type: str  # simhash | minhash | chunk_hash
    value: Any  # binary / int / hash (type-specific)
    doc_id: bytes  # SHA-256 of doc or doc identity
    chunk_id: Optional[str] = None
    source: str = ""
    language: str = "en"
    created_at: float = field(default_factory=time.time)
    hash_params: Optional[HashParams] = None

    def to_dict(self) -> Dict[str, Any]:
        doc_id_hex = self.doc_id.hex() if isinstance(self.doc_id, bytes) else str(self.doc_id)
        return {
            "fingerprint_id": self.fingerprint_id,
            "fingerprint_type": self.fingerprint_type,
            "value": self._value_repr(),
            "doc_id": doc_id_hex,
            "chunk_id": self.chunk_id,
            "source": self.source,
            "language": self.language,
            "created_at": self.created_at,
            "hash_params": (
                {k: v for k, v in self.hash_params.__dict__.items() if not k.startswith("_")}
                if self.hash_params else None
            ),
        }

    def _value_repr(self) -> Any:
        if self.fingerprint_type == FingerprintType.SIMHASH.value and isinstance(self.value, int):
            return hex(self.value)
        if isinstance(self.value, bytes):
            return self.value.hex()
        return self.value

    @classmethod
    def create(
        cls,
        fingerprint_type: str,
        value: Any,
        doc_id: bytes,
        source: str = "",
        language: str = "en",
        chunk_id: Optional[str] = None,
        hash_params: Optional[HashParams] = None,
    ) -> FingerprintRecord:
        return cls(
            fingerprint_id=str(uuid.uuid4()),
            fingerprint_type=fingerprint_type,
            value=value,
            doc_id=doc_id,
            chunk_id=chunk_id,
            source=source,
            language=language,
            created_at=time.time(),
            hash_params=hash_params,
        )


@dataclass
class DedupDecision:
    """Result of querying the global store + decision engine."""
    action: DedupAction  # drop | keep | keep_link
    match_type: Optional[str] = None  # simhash | minhash | chunk_hash
    existing_doc_ids: List[bytes] = field(default_factory=list)
    existing_sources: List[str] = field(default_factory=list)
    duplicate_chunk_ids: List[str] = field(default_factory=list)  # for partial overlap
    reason: str = ""
