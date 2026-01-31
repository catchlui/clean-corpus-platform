"""Base interface for global fingerprint stores.

Stores are persistent, append-only (mostly), and queried by every ingestion job.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from .schema import FingerprintRecord, HashParams, DedupDecision, DedupAction


class FingerprintStore(ABC):
    """Abstract global fingerprint store. One per type (simhash / minhash / chunk_hash)."""

    @property
    @abstractmethod
    def fingerprint_type(self) -> str:
        """simhash | minhash | chunk_hash."""
        pass

    @abstractmethod
    def query(
        self,
        value: Any,
        source: Optional[str] = None,
        language: Optional[str] = None,
        hash_params: Optional[HashParams] = None,
    ) -> Tuple[bool, List[FingerprintRecord]]:
        """Check if this fingerprint (or near-match) exists. Returns (found, list of matching records)."""
        pass

    @abstractmethod
    def add(self, record: FingerprintRecord) -> None:
        """Append a fingerprint to the global store (persist)."""
        pass

    @abstractmethod
    def flush(self) -> None:
        """Persist in-memory state to backend if any."""
        pass

    def get_version_params(self) -> Optional[HashParams]:
        """Return hash params used by this store (for versioning). Override in impl."""
        return None
