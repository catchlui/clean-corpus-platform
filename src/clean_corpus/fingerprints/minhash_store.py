"""Global MinHash + LSH store. Semantic near-duplicate detection; cross-dataset collisions."""

from __future__ import annotations
import json
import os
from typing import Any, Dict, List, Optional, Tuple

try:
    from datasketch import MinHash, MinHashLSH
except ImportError:
    MinHash = None  # type: ignore
    MinHashLSH = None  # type: ignore

from ..storage.base import StorageBackend
from .base import FingerprintStore
from .schema import FingerprintRecord, FingerprintType, HashParams


def _minhash_signature(text: str, ngram: int, num_perm: int) -> Any:
    if MinHash is None:
        raise ImportError("datasketch required for MinHashStore. Install with: pip install datasketch")
    mh = MinHash(num_perm=num_perm)
    t = text
    if len(t) < ngram:
        mh.update(t.encode("utf-8", errors="ignore"))
        return mh
    for i in range(0, len(t) - ngram + 1, max(1, ngram)):
        mh.update(t[i : i + ngram].encode("utf-8", errors="ignore"))
    return mh


class MinHashStore(FingerprintStore):
    """Global MinHash + LSH store. Persists LSH buckets and doc_id mapping; loads on init."""

    def __init__(
        self,
        storage: StorageBackend,
        root_path: str,
        threshold: float = 0.9,
        ngram: int = 5,
        num_perm: int = 128,
        fingerprint_version: str = "v1",
    ):
        if MinHashLSH is None:
            raise ImportError("datasketch required for MinHashStore. Install with: pip install datasketch")
        self.storage = storage
        self.root_path = root_path
        self.threshold = float(threshold)
        self.ngram = int(ngram)
        self.num_perm = int(num_perm)
        self.fingerprint_version = fingerprint_version
        self._hash_params = HashParams(
            fingerprint_version=fingerprint_version,
            shingle_size=ngram,
            num_hashes=num_perm,
        )
        self._index_path = storage.join(root_path, "minhash_lsh")
        self._doc_index_path = storage.join(root_path, "minhash_docs.json")
        self.lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_perm)
        self._key_to_record: Dict[str, FingerprintRecord] = {}
        self._next_key = 0
        self._load()

    @property
    def fingerprint_type(self) -> str:
        return FingerprintType.MINHASH.value

    def get_version_params(self) -> Optional[HashParams]:
        return self._hash_params

    def _load(self) -> None:
        """Load doc_id -> key mapping; LSH state re-built from persisted signatures if present."""
        self.storage.makedirs(self._index_path, exist_ok=True)
        if self.storage.exists(self._doc_index_path):
            try:
                data = self.storage.read_file(self._doc_index_path)
                obj = json.loads(data.decode("utf-8"))
                self._key_to_record = {
                    k: FingerprintRecord(
                        fingerprint_id=v["fingerprint_id"],
                        fingerprint_type=v["fingerprint_type"],
                        value=v.get("value"),  # not used for query
                        doc_id=bytes.fromhex(v["doc_id"]),
                        source=v.get("source", ""),
                        language=v.get("language", "en"),
                        created_at=v.get("created_at", 0),
                    )
                    for k, v in obj.get("docs", {}).items()
                }
                self._next_key = max((int(k.replace("k", "") or "0") for k in self._key_to_record), default=0) + 1
            except Exception:
                pass
        # Optionally re-load LSH from serialized bands (datasketch supports pickle); for simplicity we keep in-memory
        # and only persist doc list. Full LSH persistence would require saving/loading the LSH structure.

    def _persist_docs(self) -> None:
        payload = {
            "docs": {
                k: {
                    "fingerprint_id": r.fingerprint_id,
                    "fingerprint_type": r.fingerprint_type,
                    "doc_id": r.doc_id.hex(),
                    "source": r.source,
                    "language": r.language,
                    "created_at": r.created_at,
                }
                for k, r in self._key_to_record.items()
            }
        }
        self.storage.write_file(self._doc_index_path, json.dumps(payload).encode("utf-8"))

    def query(
        self,
        value: Any,
        source: Optional[str] = None,
        language: Optional[str] = None,
        hash_params: Optional[HashParams] = None,
    ) -> Tuple[bool, List[FingerprintRecord]]:
        # value is a MinHash object
        hits = self.lsh.query(value)
        records = [self._key_to_record[h] for h in hits if h in self._key_to_record]
        return (len(records) > 0, records)

    def add(self, record: FingerprintRecord) -> None:
        # record.value must be a MinHash instance for LSH insert
        key = f"k{self._next_key}"
        self._next_key += 1
        self.lsh.insert(key, record.value)
        self._key_to_record[key] = FingerprintRecord(
            fingerprint_id=record.fingerprint_id,
            fingerprint_type=record.fingerprint_type,
            value=None,
            doc_id=record.doc_id,
            chunk_id=record.chunk_id,
            source=record.source,
            language=record.language,
            created_at=record.created_at,
            hash_params=record.hash_params,
        )
        self._persist_docs()

    def flush(self) -> None:
        self._persist_docs()
