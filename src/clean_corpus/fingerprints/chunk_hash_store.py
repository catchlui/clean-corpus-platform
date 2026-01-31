"""Global chunk hash store (LLM-critical). Prevents memorization; identical chunks appear once."""

from __future__ import annotations
import json
from typing import Any, List, Optional, Tuple

from ..storage.base import StorageBackend
from ..utils.hashing import sha256_bytes
from .base import FingerprintStore
from .schema import FingerprintRecord, FingerprintType, HashParams


def _normalize_chunk(text: str) -> str:
    """Light normalization: strip, collapse whitespace."""
    return " ".join(text.split())


def chunk_text(text: str, chunk_size: int = 512, overlap: int = 0) -> List[str]:
    """Split text into overlapping chunks (by chars). For production use token-based chunking."""
    text = _normalize_chunk(text)
    if not text or chunk_size <= 0:
        return [text] if text else []
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start = end - overlap if overlap else end
    return chunks


class ChunkHashStore(FingerprintStore):
    """Global chunk hash store. Key: SHA-256 of normalized chunk. Query: exact match."""

    def __init__(
        self,
        storage: StorageBackend,
        root_path: str,
        chunk_size: int = 512,
        chunk_overlap: int = 0,
        fingerprint_version: str = "v1",
    ):
        self.storage = storage
        self.root_path = root_path
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.fingerprint_version = fingerprint_version
        self._hash_params = HashParams(
            fingerprint_version=fingerprint_version,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )
        self._index_path = storage.join(root_path, "chunk_hash")
        self._cache: dict = {}  # hash_bytes -> [FingerprintRecord]
        self._load()

    @property
    def fingerprint_type(self) -> str:
        return FingerprintType.CHUNK_HASH.value

    def get_version_params(self) -> Optional[HashParams]:
        return self._hash_params

    def _load(self) -> None:
        self.storage.makedirs(self._index_path, exist_ok=True)
        for f in self.storage.list_files(self._index_path, "*.json") or []:
            try:
                data = self.storage.read_file(f)
                obj = json.loads(data.decode("utf-8"))
                h = bytes.fromhex(obj["value_hex"])
                rec = FingerprintRecord(
                    fingerprint_id=obj["fingerprint_id"],
                    fingerprint_type=obj["fingerprint_type"],
                    value=h,
                    doc_id=bytes.fromhex(obj["doc_id"]),
                    chunk_id=obj.get("chunk_id"),
                    source=obj.get("source", ""),
                    language=obj.get("language", "en"),
                    created_at=obj.get("created_at", 0),
                )
                self._cache.setdefault(h, []).append(rec)
            except Exception:
                continue

    def _persist_one(self, record: FingerprintRecord) -> None:
        h = record.value if isinstance(record.value, bytes) else bytes.fromhex(record.value)
        path = self.storage.join(self._index_path, f"{h.hex()}.json")
        payload = {
            "fingerprint_id": record.fingerprint_id,
            "fingerprint_type": record.fingerprint_type,
            "value_hex": h.hex(),
            "doc_id": record.doc_id.hex(),
            "chunk_id": record.chunk_id,
            "source": record.source,
            "language": record.language,
            "created_at": record.created_at,
        }
        self.storage.write_file(path, json.dumps(payload).encode("utf-8"))

    def query(
        self,
        value: Any,
        source: Optional[str] = None,
        language: Optional[str] = None,
        hash_params: Optional[HashParams] = None,
    ) -> Tuple[bool, List[FingerprintRecord]]:
        h = value if isinstance(value, bytes) else bytes.fromhex(value) if isinstance(value, str) else value
        records = self._cache.get(h, [])
        return (len(records) > 0, list(records))

    def add(self, record: FingerprintRecord) -> None:
        h = record.value if isinstance(record.value, bytes) else bytes.fromhex(record.value)
        self._cache.setdefault(h, []).append(record)
        self._persist_one(record)

    def flush(self) -> None:
        pass

    @staticmethod
    def compute_chunk_hashes(text: str, chunk_size: int = 512, overlap: int = 0) -> List[Tuple[bytes, str]]:
        """Returns [(chunk_hash, chunk_id)] for each chunk. chunk_id is index-based."""
        chunks = chunk_text(text, chunk_size, overlap)
        result = []
        for i, c in enumerate(chunks):
            normalized = _normalize_chunk(c)
            h = sha256_bytes(normalized)
            result.append((h, str(i)))
        return result
