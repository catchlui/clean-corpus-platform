"""Global Fingerprint Layer: persistent, queryable across all datasets and time."""

from .schema import (
    FingerprintRecord,
    FingerprintType,
    HashParams,
    DedupDecision,
    DedupAction,
)
from .base import FingerprintStore
from .simhash_store import SimHashStore
from .minhash_store import MinHashStore
from .chunk_hash_store import ChunkHashStore
from .manager import GlobalFingerprintManager
from .metrics import FingerprintMetrics

__all__ = [
    "FingerprintRecord",
    "FingerprintType",
    "HashParams",
    "DedupDecision",
    "DedupAction",
    "FingerprintStore",
    "SimHashStore",
    "MinHashStore",
    "ChunkHashStore",
    "GlobalFingerprintManager",
    "FingerprintMetrics",
]
